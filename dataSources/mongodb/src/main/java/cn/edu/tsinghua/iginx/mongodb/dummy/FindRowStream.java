package cn.edu.tsinghua.iginx.mongodb.dummy;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.mongodb.client.MongoCursor;
import java.util.*;
import org.bson.BsonDocument;

class FindRowStream implements RowStream {
  private final MongoCursor<BsonDocument> cursor;
  private final PathTree tree;
  private final Map<String, DataType> projectedSchema;
  private final Header header;
  private final String[] prefixes;

  public FindRowStream(
      MongoCursor<BsonDocument> cursor,
      PathTree tree,
      Map<String, DataType> projectedSchema,
      String[] prefixes) {
    this.cursor = cursor;
    this.tree = tree;
    this.projectedSchema = projectedSchema;
    this.prefixes = prefixes;

    String prefix = String.join(".", prefixes);
    List<Field> fields = new ArrayList<>();
    for (Map.Entry<String, DataType> entry : projectedSchema.entrySet()) {
      fields.add(new Field(prefix + "." + entry.getKey(), entry.getValue()));
    }
    this.header = new Header(Field.KEY, fields);
  }

  @Override
  public Header getHeader() {
    return header;
  }

  @Override
  public void close() throws PhysicalException {
    cursor.close();
  }

  private long lastRecordId = 0;
  private Iterator<Row> nextRows = Collections.emptyIterator();

  @Override
  public boolean hasNext() {
    if (!nextRows.hasNext()) {
      fetchNextRows();
    }
    return nextRows.hasNext();
  }

  @Override
  public Row next() throws PhysicalException {
    if (!nextRows.hasNext()) {
      throw new NoSuchElementException();
    }
    return nextRows.next();
  }

  private void fetchNextRows() {
    if (!cursor.hasNext()) return;

    ResultTable.Builder builder = new ResultTable.Builder();
    BsonDocument doc = cursor.next();
    lastRecordId++;
    builder.add(lastRecordId << 32, doc, tree);

    ResultTable table = builder.build(prefixes, projectedSchema);
    Map<String, ResultColumn> columns = table.getColumns();

    SortedSet<Long> sortedKeys = new TreeSet<>();
    for (ResultColumn column : columns.values()) {
      sortedKeys.addAll(column.getData().keySet());
    }

    List<Row> rows = new ArrayList<>();
    for (long key : sortedKeys) {
      Object[] values = new Object[header.getFieldSize()];
      for (int i = 0; i < header.getFieldSize(); i++) {
        Field field = header.getField(i);
        String path = field.getName();
        ResultColumn column = columns.get(path);
        if (column != null) {
          values[i] = column.getData().getOrDefault(key, null);
        }
      }
      rows.add(new Row(header, key, values));
    }
    nextRows = rows.iterator();
  }
}
