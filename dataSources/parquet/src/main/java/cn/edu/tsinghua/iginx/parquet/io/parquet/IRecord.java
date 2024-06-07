/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

  public void sort() {
    values.sort(Comparator.comparingInt(Map.Entry::getKey));
  }
}
