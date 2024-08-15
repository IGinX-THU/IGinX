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
package cn.edu.tsinghua.iginx.filestore.format.parquet;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.filestore.test.DataValidator;
import com.google.common.io.MoreFiles;
import java.io.IOException;
import java.nio.file.Path;
import shaded.iginx.org.apache.parquet.schema.MessageType;

public class ParquetTestUtils {
  public static void createFile(Path path, Table table) throws IOException {
    MoreFiles.createParentDirectories(path);
    MessageType schema = ProjectUtils.toMessageType(table.getHeader());
    IParquetWriter.Builder writerBuilder = IParquetWriter.builder(path, schema);
    try (IParquetWriter writer = writerBuilder.build()) {
      for (Row row : table.getRows()) {
        Row stringAsBinary = DataValidator.withStringAsBinary(row);
        IRecord record = ProjectUtils.toRecord(stringAsBinary);
        writer.write(record);
      }
    }
  }
}
