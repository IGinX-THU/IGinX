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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.tombstone;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/** Tombstone 用于记录被删除的数据范围。 实现了 Serializable 接口，支持标准的 Java 序列化。 */
public class Tombstone implements Serializable {

  private final HashMap<Field, TreeRangeSet<Long>> keyRanges = new HashMap<>();

  public void add(Field field, RangeSet<Long> ranges) {
    if (!keyRanges.containsKey(field)) {
      keyRanges.put(field, TreeRangeSet.create());
    }
    RangeSet<Long> rangeSet = keyRanges.get(field);
    if (rangeSet != null) {
      rangeSet.addAll(ranges);
    }
  }

  public void add(Field field) {
    keyRanges.put(field, null);
  }

  public Map<Field, TreeRangeSet<Long>> getKeyRanges() {
    return keyRanges;
  }

  public static Tombstone merge(Tombstone... tombstones) {
    Tombstone merged = new Tombstone();
    for (Tombstone tombstone : tombstones) {
      for (Map.Entry<Field, TreeRangeSet<Long>> entry : tombstone.getKeyRanges().entrySet()) {
        Field field = entry.getKey();
        RangeSet<Long> ranges = entry.getValue();
        if (ranges == null) {
          merged.add(field);
        } else {
          merged.add(field, ranges);
        }
      }
    }
    return merged;
  }

  @Override
  public String toString() {
    return "Tombstone{" + "keyRanges=" + keyRanges + '}';
  }
}
