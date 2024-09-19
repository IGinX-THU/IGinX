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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.filesystem.format.parquet;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.filesystem.common.FileSystemException;
import cn.edu.tsinghua.iginx.filesystem.common.FileSystemRowStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import javax.annotation.WillCloseWhenClosed;
import shaded.iginx.org.apache.parquet.schema.MessageType;

public class ParquetFormatRowStream extends FileSystemRowStream {

  private final IParquetReader reader;
  private final Header header;
  private Row nextRow;

  public ParquetFormatRowStream(
      @WillCloseWhenClosed IParquetReader reader, Function<String, String> nameMapper)
      throws IOException {
    this.reader = reader;
    this.header = new Header(Field.KEY, toFields(reader.getSchema(), nameMapper));
    this.nextRow = fetchNext();
  }

  private static List<Field> toFields(MessageType schema, Function<String, String> nameMapper) {
    List<Field> fields = ProjectUtils.toFields(schema);
    List<Field> result = new ArrayList<>();
    for (Field field : fields) {
      String rawName = field.getName();
      String fullName = nameMapper.apply(rawName);
      result.add(new Field(fullName, field.getType()));
    }
    return result;
  }

  private Row fetchNext() throws IOException {
    IRecord record = reader.read();
    if (record == null) {
      return null;
    }
    long key = reader.getCurrentRowIndex();
    return ProjectUtils.toRow(header, key, record);
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

  @Override
  public boolean hasNext() throws FileSystemException {
    return nextRow != null;
  }

  @Override
  public Row next() throws FileSystemException {
    if (nextRow == null) {
      throw new FileSystemException("No more rows");
    }
    Row row = nextRow;
    try {
      nextRow = fetchNext();
    } catch (IOException e) {
      throw new FileSystemException(e);
    }
    return row;
  }
}
