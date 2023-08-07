package cn.edu.tsinghua.iginx.filesystem.query;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.file.property.FilePath;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class FileSystemQueryRowStream implements RowStream {
  private final Header header;
  private final List<FSResultTable> rowData;
  private final int[] indices;
  private int hasMoreRecords = 0;
  private Filter filter;
  private Row nextRow = null;

  public FileSystemQueryRowStream(
      List<FSResultTable> result, String storageUnit, String root, Filter filter) {
    Field time = Field.KEY;
    List<Field> fields = new ArrayList<>();

    this.filter = filter;
    this.rowData = result;

    String series;
    for (FSResultTable resultTable : rowData) {
      File file = resultTable.getFile();
      series =
          FilePath.convertAbsolutePathToSeries(
              root, file.getAbsolutePath(), file.getName(), storageUnit);
      Field field = new Field(series, resultTable.getDataType(), resultTable.getTags());
      fields.add(field);
    }

    this.indices = new int[this.rowData.size()];
    this.header = new Header(time, fields);
    for (int i = 0; i < this.rowData.size(); i++) {
      if (this.rowData.get(i).getVal().size() != 0) hasMoreRecords++;
    }
  }

  @Override
  public Header getHeader() throws PhysicalException {
    return header;
  }

  @Override
  public void close() throws PhysicalException {
    // need to do nothing
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

  private Row getNext() throws PhysicalException {
    long timestamp = Long.MAX_VALUE;
    for (int i = 0; i < this.rowData.size(); i++) {
      int index = indices[i];
      List<Record> records = this.rowData.get(i).getVal();
      if (index == records.size()) { // 数据已经消费完毕了
        continue;
      }
      Record record = records.get(index);
      timestamp = Math.min(record.getKey(), timestamp);
    }
    if (timestamp == Long.MAX_VALUE) {
      return null;
    }
    Object[] values = new Object[rowData.size()];
    for (int i = 0; i < this.rowData.size(); i++) {
      int index = indices[i];
      List<Record> records = this.rowData.get(i).getVal();
      if (index == records.size()) { // 数据已经消费完毕了
        continue;
      }
      Record record = records.get(index);
      if (record.getKey() == timestamp) { // 考虑时间 ns may fix it
        Object value = record.getRawData();
        values[i] = value;
        indices[i]++;
        if (indices[i] == records.size()) {
          hasMoreRecords--;
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
