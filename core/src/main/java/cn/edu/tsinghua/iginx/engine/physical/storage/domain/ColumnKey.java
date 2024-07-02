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
package cn.edu.tsinghua.iginx.engine.physical.storage.domain;

import java.util.*;

public class ColumnKey {
  private final String path;
  private final SortedMap<String, String> tags;

  public ColumnKey(String path, Map<String, String> tagList) {
    this.path = Objects.requireNonNull(path);
    this.tags = Collections.unmodifiableSortedMap(new TreeMap<>(tagList));
  }

  public String getPath() {
    return path;
  }

  public SortedMap<String, String> getTags() {
    return tags;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ColumnKey columnKey = (ColumnKey) o;
    return Objects.equals(path, columnKey.path) && Objects.equals(tags, columnKey.tags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, tags);
  }

  @Override
  public String toString() {
    return path + tags;
  }
}
