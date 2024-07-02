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
package cn.edu.tsinghua.iginx.engine.shared.data.read;

import cn.edu.tsinghua.iginx.constant.GlobalConstant;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.TagKVUtils;
import java.util.*;

public final class Field {

  public static final Field KEY = new Field();

  private final String name;

  private final String fullName;

  private final Map<String, String> tags;

  private final DataType type;

  public Field() {
    this(GlobalConstant.KEY_NAME, DataType.LONG, Collections.emptyMap());
  }

  public Field(String name, DataType type) {
    this(name, type, Collections.emptyMap());
  }

  public Field(String name, DataType type, Map<String, String> tags) {
    this(name, TagKVUtils.toFullName(name, tags), type, tags);
  }

  public Field(String name, String fullName, DataType type) {
    this(name, fullName, type, Collections.emptyMap());
  }

  private Field(String name, String fullName, DataType type, Map<String, String> tags) {
    this.name = Objects.requireNonNull(name);
    this.fullName = Objects.requireNonNull(fullName);
    this.type = Objects.requireNonNull(type);
    this.tags = Optional.ofNullable(tags).orElse(Collections.emptyMap());
  }

  public String getName() {
    return name;
  }

  public String getFullName() {
    return fullName;
  }

  public DataType getType() {
    return type;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", Field.class.getSimpleName() + "{", "}")
        .add("name='" + name + "'")
        .add("fullName='" + fullName + "'")
        .add("tags=" + tags)
        .add("type=" + type)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Field that = (Field) o;
    return Objects.equals(name, that.name)
        && Objects.equals(fullName, that.fullName)
        && type == that.type
        && Objects.equals(tags, that.tags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, fullName, type, tags);
  }
}
