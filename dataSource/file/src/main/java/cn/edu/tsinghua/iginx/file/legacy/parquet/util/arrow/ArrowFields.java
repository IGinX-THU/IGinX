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
package cn.edu.tsinghua.iginx.file.legacy.parquet.util.arrow;

import cn.edu.tsinghua.iginx.engine.physical.storage.domain.ColumnKey;
import cn.edu.tsinghua.iginx.file.legacy.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.file.legacy.parquet.manager.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.file.legacy.parquet.util.Constants;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

public class ArrowFields {

  public static Field of(
      boolean nullable,
      ColumnKey columnKey,
      DataType type,
      @Nullable DictionaryEncoding dictionaryEncoding,
      @Nullable List<Field> children) {
    Preconditions.checkNotNull(columnKey, "columnKey");
    Preconditions.checkNotNull(type, "type");

    Map<String, String> tags = columnKey.getTags();
    Map<String, String> metadata = tags.isEmpty() ? null : tags;
    FieldType fieldType = ArrowFieldTypes.of(nullable, type, dictionaryEncoding, metadata);
    return new Field(columnKey.getPath(), fieldType, children);
  }

  public static Field of(boolean nullable, ColumnKey columnKey, DataType type) {
    return of(nullable, columnKey, type, null, null);
  }

  public static Field of(ColumnKey columnKey, DataType type) {
    return of(true, columnKey, type);
  }

  public static ColumnKey toColumnKey(Field field) {
    Preconditions.checkNotNull(field, "field");
    Preconditions.checkArgument(!field.getType().isComplex());

    return new ColumnKey(field.getName(), field.getMetadata());
  }

  public static Field KEY = of(false, new ColumnKey(Constants.KEY_FIELD_NAME), DataType.LONG);

  public static Set<Field> of(Map<String, DataType> schema) {
    return schema.entrySet().stream()
        .map(entry -> of(TagKVUtils.splitFullName(entry.getKey()), entry.getValue()))
        .collect(Collectors.toSet());
  }

  public static Map<String, DataType> toIginxSchema(Set<Field> fields) {
    return fields.stream()
        .collect(
            Collectors.toMap(
                field -> TagKVUtils.toFullName(toColumnKey(field)),
                field -> ArrowTypes.toIginxType(field.getType())));
  }

  public static AreaSet<Long, String> toInnerAreas(AreaSet<Long, Field> areas) {
    AreaSet<Long, String> innerAreas = new AreaSet<>();
    innerAreas.add(areas.getKeys());
    Set<String> innerFields =
        areas.getFields().stream().map(ArrowFields::toFullName).collect(Collectors.toSet());
    innerAreas.add(innerFields);
    areas
        .getSegments()
        .forEach(
            (field, rangeSet) -> {
              String innerField = toFullName(field);
              innerAreas.add(Collections.singleton(innerField), rangeSet);
            });
    return innerAreas;
  }

  public static String toFullName(Field field) {
    return TagKVUtils.toFullName(toColumnKey(field));
  }

  public static AreaSet<Long, Field> of(AreaSet<Long, String> areas, Map<String, DataType> schema) {
    AreaSet<Long, Field> arrowAreas = new AreaSet<>();
    arrowAreas.add(areas.getKeys());
    Set<Field> fields =
        areas.getFields().stream()
            .map(name -> of(TagKVUtils.splitFullName(name), schema.get(name)))
            .collect(Collectors.toSet());
    arrowAreas.add(fields);
    areas
        .getSegments()
        .forEach(
            (name, rangeSet) -> {
              Field arrowField = of(TagKVUtils.splitFullName(name), schema.get(name));
              arrowAreas.add(Collections.singleton(arrowField), rangeSet);
            });
    return arrowAreas;
  }

  public static Field notNull(Field field) {
    return of(false, toColumnKey(field), ArrowTypes.toIginxType(field.getType()));
  }

  public static Field nullable(Field field) {
    return of(true, toColumnKey(field), ArrowTypes.toIginxType(field.getType()));
  }

  public static Set<String> toInnerFields(List<Field> fields) {
    return fields.stream()
        .map(field -> TagKVUtils.toFullName(ArrowFields.toColumnKey(field)))
        .collect(Collectors.toSet());
  }
}
