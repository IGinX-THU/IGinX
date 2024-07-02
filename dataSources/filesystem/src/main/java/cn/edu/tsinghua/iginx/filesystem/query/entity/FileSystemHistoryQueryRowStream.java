/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.filesystem.query.entity;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.exception.FilesystemException;
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

  private MemoryPool memoryPool;

  // may fix it ，可能可以不用传pathMap
  public FileSystemHistoryQueryRowStream(
      List<FileSystemResultTable> result, String root, Filter filter, MemoryPool memoryPool) {
    Field time = Field.KEY;
    List<Field> fields = new ArrayList<>();

    this.filter = filter;
    this.rowData = result;
    this.memoryPool = memoryPool;

    String column;
    for (FileSystemResultTable resultTable : rowData) {
      File file = resultTable.getFile();
      column = FilePathUtils.convertAbsolutePathToPath(root, file.getAbsolutePath(), null);
      Field field = new Field(column, resultTable.getDataType(), resultTable.getTags());
      fields.add(field);
    }

    this.indices = new int[rowData.size()][];
    for (int i = 0; i < rowData.size(); i++) {
      int recordSize = rowData.get(i).getRecords().size() + 10;
      indices[i] = new int[recordSize];
    }
    this.round = new int[rowData.size()];
    this.header = new Header(time, fields);
    for (FileSystemResultTable row : rowData) {
      if (!row.getRecords().isEmpty()) {
        hasMoreRecords++;
      }
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
      throw new FilesystemException("no more data");
    }
    Row currRow = nextRow;
    nextRow = calculateNext();
    return currRow;
  }

  public Row getNext() throws PhysicalException {
    long key = Long.MAX_VALUE;
    for (int i = 0; i < rowData.size(); i++) {
      int index = round[i];
      List<Record> records = rowData.get(i).getRecords();
      if (index == records.size()) { // 数据已经消费完毕了
        continue;
      }
      key = Math.min(records.get(index).getKey(), key);
    }
    if (key == Long.MAX_VALUE) {
      return null;
    }
    Object[] values = new Object[rowData.size()];
    for (int i = 0; i < rowData.size(); i++) {
      int columnIndex = round[i];
      List<Record> records = rowData.get(i).getRecords();
      if (columnIndex == records.size()) { // 数据已经消费完毕了
        continue;
      }
      byte[] value = (byte[]) records.get(columnIndex).getRawData();
      if (records.get(columnIndex).getKey() == key) {
        values[i] = value;
        indices[i][columnIndex] += value.length;
        if (indices[i][columnIndex] >= value.length) {
          round[i]++;
          if (round[i] == records.size()) {
            hasMoreRecords--;
          }
        }
      }
    }
    return new Row(header, key, values);
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
