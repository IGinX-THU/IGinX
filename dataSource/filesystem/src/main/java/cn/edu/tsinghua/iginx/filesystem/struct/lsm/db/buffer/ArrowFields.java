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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer;

import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

public class ArrowFields {

  public static Field of(String name, Map<String, String> tags, DataType type) {
    return of(name, tags, ArrowTypes.minorTypeOf(type));
  }

  public static Field of(String name, Map<String, String> tags, Types.MinorType minorType) {
    return of(true, name, tags, minorType.getType());
  }

  public static Field of(boolean nullable, String name, Map<String, String> tags, ArrowType type) {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(tags);
    Preconditions.checkNotNull(type);

    FieldType fieldType = new FieldType(nullable, type, null, ImmutableSortedMap.copyOf(tags));
    return new Field(name, fieldType, null);
  }

  public static Field KEY =
      of(false, Constants.KEY, Collections.emptyMap(), Types.MinorType.BIGINT.getType());

  public static cn.edu.tsinghua.iginx.engine.shared.data.read.Field toIginxField(Field arrowField) {
    return new cn.edu.tsinghua.iginx.engine.shared.data.read.Field(
        arrowField.getName(),
        ArrowTypes.toIginxType(arrowField.getType()),
        arrowField.getMetadata());
  }

  public static Field of(cn.edu.tsinghua.iginx.engine.shared.data.read.Field field) {
    return of(field.getName(), field.getTags(), field.getType());
  }

  public static List<Field> of(List<cn.edu.tsinghua.iginx.engine.shared.data.read.Field> fields) {
    return fields.stream().map(ArrowFields::of).collect(ImmutableList.toImmutableList());
  }
}
