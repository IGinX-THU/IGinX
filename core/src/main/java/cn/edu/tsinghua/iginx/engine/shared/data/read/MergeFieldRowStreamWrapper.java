package cn.edu.tsinghua.iginx.engine.shared.data.read;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import java.util.*;

public class MergeFieldRowStreamWrapper implements RowStream {

  private final PriorityQueue<Entry> queue = new PriorityQueue<>();
  private final Header header;

  public MergeFieldRowStreamWrapper(List<RowStream> rowStreams) throws PhysicalException {
    List<Field> distFields = new ArrayList<>();

    Map<String, Integer> indexMap = new HashMap<>();
    for (int i = 0; i < rowStreams.size(); i++) {
      RowStream rowStream = rowStreams.get(i);

      Header header = rowStream.getHeader();
      if (!header.hasKey()) {
        throw new IllegalArgumentException("The row stream must have a key");
      }
      List<Field> fields = header.getFields();
      int[] distIndexes = new int[fields.size()];
      for (int index = 0; index < fields.size(); index++) {
        Field field = fields.get(index);

        if (!indexMap.containsKey(field.getName())) {
          int newDistIndex = distFields.size();
          indexMap.put(field.getName(), newDistIndex);
          distIndexes[index] = newDistIndex;
          distFields.add(field);
          continue;
        }

        int distIndex = indexMap.get(field.getName());
        Field distField = distFields.get(distIndex);
        if (!field.getType().equals(distField.getType())) {
          distIndexes[index] = -1;
        } else {
          distIndexes[index] = distIndex;
        }
      }

      Entry entry = new Entry(i, rowStream, distIndexes);
      if (entry.fetch()) {
        queue.add(entry);
      } else {
        entry.rowStream.close();
      }
    }

    header = new Header(Field.KEY, distFields);
  }

  @Override
  public Header getHeader() throws PhysicalException {
    return header;
  }

  @Override
  public void close() throws PhysicalException {
    for (Entry entry : queue) {
      entry.rowStream.close();
    }
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    return !queue.isEmpty();
  }

  @Override
  public Row next() throws PhysicalException {
    if (queue.isEmpty()) {
      throw new NoSuchElementException("The row stream has used up");
    }

    long key = queue.peek().getKey();
    Object[] values = new Object[header.getFieldSize()];

    while (!queue.isEmpty()) {
      long nextKey = queue.peek().getKey();
      if (nextKey < key) {
        throw new IllegalStateException("The key is not in order");
      }
      if (nextKey > key) {
        break;
      }
      Entry entry = queue.poll();
      assert entry != null;
      entry.fill(values);
      if (entry.fetch()) {
        queue.add(entry);
      }
    }

    return new Row(header, key, values);
  }

  private static class Entry implements Comparable<Entry> {
    private final int[] indexes;
    private final int order;
    private final RowStream rowStream;

    public Entry(int order, RowStream rowStream, int[] indexes) throws PhysicalException {
      this.order = order;
      this.rowStream = rowStream;
      this.indexes = indexes;
    }

    private Row nextRow;

    public boolean fetch() throws PhysicalException {
      if (rowStream.hasNext()) {
        nextRow = rowStream.next();
        return true;
      }
      nextRow = null;
      return false;
    }

    public void fill(Object[] dist) throws PhysicalException {
      Object[] src = nextRow.getValues();
      for (int i = 0; i < indexes.length; i++) {
        int index = indexes[i];
        if (index != -1 && src[i] != null) {
          dist[index] = src[i];
        }
      }
    }

    public long getKey() {
      return nextRow.getKey();
    }

    @Override
    public int compareTo(Entry o) {
      int keyCompare = Long.compare(getKey(), o.getKey());
      if (keyCompare != 0) {
        return keyCompare;
      }
      return Integer.compare(order, o.order);
    }
  }
}
