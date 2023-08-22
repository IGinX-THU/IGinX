package cn.edu.tsinghua.iginx.filesystem.query.entity;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.tools.FilePathUtils;
import cn.edu.tsinghua.iginx.filesystem.tools.MemoryPool;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class FileSystemHistoryQueryRowStream implements RowStream {
  private final Header header;
  private final List<FileSystemResultTable> rowData;
  private final int[][] indices;
  private final int[] round;
  private int hasMoreRecords = 0;
  private Filter filter;
  private Row nextRow = null;
  private MemoryPool memoryPool = null;

  public FileSystemHistoryQueryRowStream() {
    Field time = Field.KEY;
    List<Field> fields = new ArrayList<>();

    this.rowData = new ArrayList<>();
    this.indices = new int[0][1024 * 1024 + 100];
    this.round = new int[0];
    this.header = new Header(time, fields);
    for (int i = 0; i < this.rowData.size(); i++) {
      if (this.rowData.get(i).getRecords().size() != 0) hasMoreRecords++;
    }
  }

  // may fix it ，可能可以不用传pathMap
  public FileSystemHistoryQueryRowStream(
      List<FileSystemResultTable> result, String root, Filter filter, MemoryPool memoryPool) {
    Field time = Field.KEY;
    List<Field> fields = new ArrayList<>();

    this.filter = filter;
    this.rowData = result;
    this.memoryPool = memoryPool;

    String series;
    for (FileSystemResultTable resultTable : rowData) {
      File file = resultTable.getFile();
      series = FilePathUtils.convertAbsolutePathToPath(root, file.getAbsolutePath(), null);
      Field field = new Field(series, resultTable.getDataType(), resultTable.getTags());
      fields.add(field);
    }

    this.indices = new int[this.rowData.size()][1024 * 100];
    this.round = new int[this.rowData.size()];
    this.header = new Header(time, fields);
    for (int i = 0; i < this.rowData.size(); i++) {
      if (this.rowData.get(i).getRecords().size() != 0) hasMoreRecords++;
    }
  }

  @Override
  public Header getHeader() throws PhysicalException {
    return header;
  }

  @Override
  public void close() throws PhysicalException {
    // release the memory
    for (FileSystemResultTable table : rowData) {
      List<Record> records = table.getRecords();
      for (Record record : records) {
        memoryPool.release((byte[]) record.getRawData());
      }
    }
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    if (nextRow == null && hasMoreRecords != 0) {
      nextRow = calculateNext();
    }
    return nextRow != null;
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new PhysicalException("no more data");
    }
    Row currRow = nextRow;
    nextRow = calculateNext();
    return currRow;
  }

  public Row getNext() throws PhysicalException {
    long timestamp = Long.MAX_VALUE;
    for (int i = 0; i < this.rowData.size(); i++) {
      int index = round[i];
      List<Record> records = this.rowData.get(i).getRecords();
      if (index == records.size()) { // 数据已经消费完毕了
        continue;
      }
      timestamp = Math.min(records.get(index).getKey(), timestamp);
    }
    if (timestamp == Long.MAX_VALUE) {
      return null;
    }
    Object[] values = new Object[rowData.size()];
    for (int i = 0; i < this.rowData.size(); i++) {
      int columnIndex = round[i];
      List<Record> records = this.rowData.get(i).getRecords();
      if (columnIndex == records.size()) { // 数据已经消费完毕了
        continue;
      }
      byte[] val = (byte[]) records.get(columnIndex).getRawData();
      if (records.get(columnIndex).getKey() == timestamp) {
        Object value = val;
        values[i] = value;
        indices[i][columnIndex] += val.length;
        if (indices[i][columnIndex] >= val.length) {
          round[i]++;
          if (round[i] == records.size()) hasMoreRecords--;
        }
      }
    }
    return new Row(header, timestamp, values);
  }

  private Row calculateNext() throws PhysicalException {
    Row row = getNext();
    while (row != null) {
      if (filter == null || FilterUtils.validate(filter, row)) {
        return row;
      }
      row = getNext();
    }
    return null;
  }
}
