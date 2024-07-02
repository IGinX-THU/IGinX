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
package cn.edu.tsinghua.iginx.parquet.io.parquet;

import java.util.*;
import shaded.iginx.org.apache.parquet.io.api.Binary;

public class IRecord implements Iterable<Map.Entry<Integer, Object>> {
  private final List<Map.Entry<Integer, Object>> values = new ArrayList<>();

  public IRecord add(int field, Object value) {
    values.add(new AbstractMap.SimpleEntry<>(field, value));
    return this;
  }

  public int size() {
    return values.size();
  }

  @Override
  public Iterator<Map.Entry<Integer, Object>> iterator() {
    return new Iterator<Map.Entry<Integer, Object>>() {

      private final Iterator<Map.Entry<Integer, Object>> iterator = values.iterator();

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public Map.Entry<Integer, Object> next() {
        Map.Entry<Integer, Object> entry = iterator.next();
        if (entry.getValue() instanceof Binary) {
          return new AbstractMap.SimpleEntry<>(
              entry.getKey(), ((Binary) entry.getValue()).getBytes());
        }
        return entry;
      }
    };
  }
}
