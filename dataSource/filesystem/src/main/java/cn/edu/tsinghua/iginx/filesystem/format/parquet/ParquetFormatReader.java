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
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.common.IginxPaths;
import cn.edu.tsinghua.iginx.filesystem.common.Patterns;
import cn.edu.tsinghua.iginx.filesystem.common.RowStreams;
import cn.edu.tsinghua.iginx.filesystem.format.FileFormat;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.IOException;
import java.util.*;
import javax.annotation.Nullable;
import shaded.iginx.org.apache.parquet.hadoop.metadata.ParquetMetadata;

public class ParquetFormatReader implements FileFormat.Reader {

  private final String prefix;
  private final IParquetReader.Builder builder;
  private final ParquetMetadata footer;
  private final Map<String, DataType> fields = new HashMap<>();
  private final Map<String, String> fieldToRawName = new HashMap<>();
  private final Map<String, String> rawNameToField = new HashMap<>();

  public ParquetFormatReader(
      @Nullable String prefix, IParquetReader.Builder builder, ParquetMetadata footer)
      throws IOException {
    this.prefix = prefix;
    this.builder = Objects.requireNonNull(builder);
    this.footer = Objects.requireNonNull(footer);
    initSchema();
  }

  private void initSchema() throws IOException {
    List<Field> fields = ProjectUtils.toFields(footer.getFileMetaData().getSchema());
    for (Field field : fields) {
      String rawName = field.getName();
      String fullName = IginxPaths.join(prefix, rawName);
      this.fields.put(fullName, field.getType());
      this.fieldToRawName.put(fullName, rawName);
      this.rawNameToField.put(rawName, fullName);
    }
  }

  @Override
  public String toString() {
    return "ParquetFormatReader{}";
  }

  @Override
  public void close() throws IOException {}

  @Override
  public Map<String, DataType> find(Collection<String> patterns) throws IOException {
    Map<String, DataType> result = new HashMap<>();
    for (String field : fields.keySet()) {
      if (Patterns.match(patterns, field)) {
        result.put(field, fields.get(field));
      }
    }
    return result;
  }

  @Override
  public RowStream read(List<String> fields, Filter filter) throws IOException {
    Set<String> rawFields = new HashSet<>();
    for (String field : fields) {
      rawFields.add(fieldToRawName.get(field));
    }

    IParquetReader reader = builder.project(rawFields, false).build(footer);

    RowStream rowStream = new ParquetFormatRowStream(reader, rawNameToField::get);
    return RowStreams.filtered(rowStream, filter);
  }
}
