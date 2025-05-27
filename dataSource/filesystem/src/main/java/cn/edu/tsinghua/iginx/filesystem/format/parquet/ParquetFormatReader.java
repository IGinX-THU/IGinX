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

import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.common.IginxPaths;
import cn.edu.tsinghua.iginx.filesystem.common.Patterns;
import cn.edu.tsinghua.iginx.filesystem.format.FileFormat;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.IOException;
import java.util.*;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import shaded.iginx.org.apache.parquet.hadoop.metadata.ParquetMetadata;
import shaded.iginx.org.apache.parquet.schema.GroupType;
import shaded.iginx.org.apache.parquet.schema.PrimitiveType;
import shaded.iginx.org.apache.parquet.schema.Type;

public class ParquetFormatReader implements FileFormat.Reader {

  private final String prefix;
  private final IParquetReader.Builder builder;
  private final ParquetMetadata footer;

  public ParquetFormatReader(
      @Nullable String prefix, IParquetReader.Builder builder, ParquetMetadata footer) {
    this.prefix = prefix;
    this.builder = Objects.requireNonNull(builder);
    this.footer = Objects.requireNonNull(footer);
  }

  @Override
  public String toString() {
    return "ParquetFormatReader{}";
  }

  @Override
  public void close() throws IOException {}

  @Override
  public Map<String, DataType> find(Collection<String> patterns) throws IOException {
    Map<String, DataType> schema = new HashMap<>();
    for (String pattern : patterns) {
      String patternWithoutPrefix = Patterns.removePrefix(pattern, prefix);
      List<String> patternNodes = Arrays.asList(IginxPaths.split(patternWithoutPrefix));
      findGroup(footer.getFileMetaData().getSchema(), patternNodes, new ArrayList<>(), schema);
      if (Patterns.isAll(patternWithoutPrefix)) {
        break;
      }
    }
    return schema;
  }

  private void find(
      Type type, List<String> patternNodes, List<String> pathPrefix, Map<String, DataType> output) {
    if (patternNodes.isEmpty()) {
      return;
    }
    String currentNode = patternNodes.get(0);
    if (!Patterns.match(currentNode, type.getName())) {
      return;
    }
    pathPrefix.add(type.getName());
    if (Patterns.isAll(currentNode)) {
      findMayBeRepeated(type, patternNodes, pathPrefix, output);
    }
    patternNodes = patternNodes.subList(1, patternNodes.size());
    findMayBeRepeated(type, patternNodes, pathPrefix, output);
    pathPrefix.remove(pathPrefix.size() - 1);
  }

  private void findMayBeRepeated(
      Type type, List<String> patternNodes, List<String> pathPrefix, Map<String, DataType> output) {
    switch (type.getRepetition()) {
      case REQUIRED:
      case OPTIONAL:
        findWithoutRepetition(type, patternNodes, pathPrefix, output);
        break;
      case REPEATED:
        findWithRepetition(type, patternNodes, pathPrefix, output);
        break;
      default:
        throw new IllegalArgumentException("Unsupported repetition type: " + type.getRepetition());
    }
  }

  private void findWithRepetition(
      Type type, List<String> patternNodes, List<String> pathPrefix, Map<String, DataType> output) {
    if (patternNodes.isEmpty()) {
      return;
    }
    String currentNode = patternNodes.get(0);
    boolean patternIsAll = Patterns.isAll(currentNode);
    if (!patternIsAll && !StringUtils.isNumeric(currentNode)) {
      return;
    }
    pathPrefix.add(currentNode);
    if (patternIsAll) {
      findWithoutRepetition(type, patternNodes, pathPrefix, output);
    }
    patternNodes = patternNodes.subList(1, patternNodes.size());
    findWithoutRepetition(type, patternNodes, pathPrefix, output);
    pathPrefix.remove(pathPrefix.size() - 1);
  }

  private void findWithoutRepetition(
      Type type, List<String> patternNodes, List<String> pathPrefix, Map<String, DataType> output) {
    if (type.isPrimitive()) {
      findPrimitive(type.asPrimitiveType(), patternNodes, pathPrefix, output);
    } else {
      findGroup(type.asGroupType(), patternNodes, pathPrefix, output);
    }
  }

  private void findGroup(
      GroupType groupType,
      List<String> patternNodes,
      List<String> pathPrefix,
      Map<String, DataType> output) {
    for (Type type : groupType.getFields()) {
      find(type, patternNodes, pathPrefix, output);
    }
  }

  private void findPrimitive(
      PrimitiveType primitiveType,
      List<String> patternNodes,
      List<String> pathPrefix,
      Map<String, DataType> output) {
    if (!patternNodes.isEmpty()) {
      return;
    }
    DataType dataType = IParquetReader.toIginxType(primitiveType.getPrimitiveTypeName());
    String path = IginxPaths.join(prefix, IginxPaths.join(pathPrefix));
    output.put(path, dataType);
  }

  @Override
  public RowStream read(List<String> fields, Filter filter) throws IOException {
    throw new UnsupportedOperationException("Not implemented yet");
    //    Set<String> rawFields = new HashSet<>();
    //    for (String field : fields) {
    //      rawFields.add(fieldToRawName.get(field));
    //    }
    //
    //    IParquetReader reader = builder.project(rawFields, false).build(footer);
    //
    //    RowStream rowStream = new ParquetFormatRowStream(reader, rawNameToField::get);
    //    return RowStreams.filtered(rowStream, filter);
  }
}
