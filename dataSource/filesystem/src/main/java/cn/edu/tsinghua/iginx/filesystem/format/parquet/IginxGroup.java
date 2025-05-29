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

import java.util.Arrays;
import java.util.Objects;
import shaded.iginx.org.apache.parquet.schema.GroupType;

public class IginxGroup {
  private final GroupType type;
  private final Object[] data;

  public IginxGroup(GroupType type, Object[] data) {
    this.type = Objects.requireNonNull(type);
    this.data = Objects.requireNonNull(data);
  }

  public GroupType getType() {
    return type;
  }

  public Object[] getData() {
    return data;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    IginxGroup that = (IginxGroup) o;
    return Objects.equals(type, that.type) && Objects.deepEquals(data, that.data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, Arrays.hashCode(data));
  }

  @Override
  public String toString() {
    return "IginxGroup{" + "type=" + type + ", data=" + Arrays.toString(data) + '}';
  }
}
