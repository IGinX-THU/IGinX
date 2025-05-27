/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.filesystem.format.parquet;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.filesystem.common.FileSystemException;
import cn.edu.tsinghua.iginx.filesystem.common.FileSystemRowStream;
import java.io.IOException;
import javax.annotation.Nullable;
import javax.annotation.WillCloseWhenClosed;
import shaded.iginx.org.apache.parquet.schema.MessageType;

public class ParquetFormatRowStream extends FileSystemRowStream {

  private final IginxParquetReader reader;
  private final MessageType projectedSchema;
  private final Header header;

  public ParquetFormatRowStream(
      @WillCloseWhenClosed IginxParquetReader reader, @Nullable String prefix) {
    this.reader = reader;
    this.projectedSchema = reader.getProjectedSchema();
    this.header = new Header(Field.KEY, ParquetFormatReader.toFields(projectedSchema, prefix));
  }

  @Override
  public Header getHeader() throws FileSystemException {
    return header;
  }

  @Override
  public void close() throws FileSystemException {
    try {
      reader.close();
    } catch (IOException e) {
      throw new FileSystemException(e);
    }
  }

  private Row nextRow;

  @Override
  public boolean hasNext() throws FileSystemException {
    if (nextRow == null) {
      nextRow = fetchNext();
    }
    return nextRow != null;
  }

  @Override
  public Row next() throws FileSystemException {
    if (nextRow == null) {
      throw new FileSystemException("No more rows");
    }
    Row row = nextRow;
    nextRow = fetchNext();
    return row;
  }

  private Row fetchNext() throws FileSystemException {
    IginxGroup group = null;
    try {
      group = reader.read();
    } catch (IOException e) {
      throw new FileSystemException(e);
    }
    if (group == null) {
      return null;
    }
    long key = reader.getCurrentRowIndex();
    Object[] values = new Object[header.getFieldSize()];
    fillFlattened(group, values, 0);
    return new Row(header, key, values);
  }

  private void fillFlattened(IginxGroup group, Object[] values, int index) {
    for (Object value : group.getData()) {
      if (value instanceof IginxGroup) {
        fillFlattened((IginxGroup) value, values, index);
      } else {
        values[index++] = value;
      }
    }
  }
}
