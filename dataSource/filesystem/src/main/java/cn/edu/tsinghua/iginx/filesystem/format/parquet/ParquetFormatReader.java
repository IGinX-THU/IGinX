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
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.common.IginxPaths;
import cn.edu.tsinghua.iginx.filesystem.common.RowStreams;
import cn.edu.tsinghua.iginx.filesystem.format.FileFormat;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import shaded.iginx.org.apache.parquet.column.ColumnDescriptor;
import shaded.iginx.org.apache.parquet.hadoop.metadata.ParquetMetadata;
import shaded.iginx.org.apache.parquet.schema.*;

public class ParquetFormatReader implements FileFormat.Reader {

  private final String prefix;
  private final Path path;
  private final ParquetMetadata metadata;

  public ParquetFormatReader(@Nullable String prefix, Path path, ParquetMetadata metadata) {
    this.prefix = prefix;
    this.path = path;
    this.metadata = Objects.requireNonNull(metadata);
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
    MessageType schema = metadata.getFileMetaData().getSchema();
    Map<String, String[]> pathEachPattern =
        ParquetSchemaProjector.findParquetLeafPathEachIginxPattern(schema, patterns, prefix);
    for (Map.Entry<String, String[]> entry : pathEachPattern.entrySet()) {
      Type type = schema.getType(entry.getValue());
      PrimitiveType.PrimitiveTypeName primitiveTypeName =
          type.asPrimitiveType().getPrimitiveTypeName();
      DataType dataType = toIginxType(primitiveTypeName);
      result.put(entry.getKey(), dataType);
    }
    return result;
  }

  public static DataType toIginxType(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
    switch (primitiveTypeName) {
      case BOOLEAN:
        return DataType.BOOLEAN;
      case INT32:
        return DataType.INTEGER;
      case INT64:
        return DataType.LONG;
      case FLOAT:
        return DataType.FLOAT;
      case DOUBLE:
        return DataType.DOUBLE;
      case BINARY:
      case INT96:
      case FIXED_LEN_BYTE_ARRAY:
        return DataType.BINARY;
      default:
        throw new IllegalArgumentException("Unsupported primitive type name: " + primitiveTypeName);
    }
  }

  public static List<Field> toFields(MessageType schema, @Nullable String prefix) {
    List<Field> result = new ArrayList<>();
    for (ColumnDescriptor columnDescriptor : schema.getColumns()) {
      String name = IginxPaths.join(prefix, IginxPaths.join(columnDescriptor.getPath()));
      DataType dataType = toIginxType(columnDescriptor.getPrimitiveType().getPrimitiveTypeName());
      Field field = new Field(name, dataType);
      result.add(field);
    }
    return result;
  }

  @Override
  public RowStream read(List<String> patterns, Filter filter) throws IOException {
    MessageType schema = metadata.getFileMetaData().getSchema();
    Map<String, String[]> pathEachPattern =
        ParquetSchemaProjector.findParquetLeafPathEachIginxPattern(schema, patterns, prefix);
    MessageType projectedSchema =
        ParquetSchemaProjector.project(
            schema,
            pathEachPattern.values().stream().map(Arrays::asList).collect(Collectors.toList()));

    IginxParquetReader.Builder builder = new IginxParquetReader.Builder(path, metadata);
    builder.project(projectedSchema);

    if (!containsRepeatedField(projectedSchema)) {
      return RowStreams.filtered(new ParquetFormatRowStream(builder.build(), prefix), filter);
    }

    try (IginxParquetReader reader = builder.build()) {
      return RowStreams.filtered(
          ParquetFormatRowTable.from(reader, prefix, pathEachPattern.keySet()), filter);
    }
  }

  public static boolean containsRepeatedField(GroupType schema) {
    for (Type type : schema.getFields()) {
      if (type.isRepetition(Type.Repetition.REPEATED)) {
        return true;
      }
      if (!type.isPrimitive()) {
        if (containsRepeatedField(type.asGroupType())) {
          return true;
        }
      }
    }
    return false;
  }
}
