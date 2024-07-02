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
package cn.edu.tsinghua.iginx.parquet.db.lsm.table;

import cn.edu.tsinghua.iginx.parquet.db.lsm.api.TableMeta;
import cn.edu.tsinghua.iginx.parquet.db.util.AreaSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DeletedTableMeta<K extends Comparable<K>, F, T, V> implements TableMeta<K, F, T, V> {
  private final Map<F, T> schema;
  private final Map<F, Range<K>> ranges;

  public DeletedTableMeta(TableMeta<K, F, T, V> tableMeta, AreaSet<K, F> tombstone) {
    this.schema = new HashMap<>(tableMeta.getSchema());
    schema.keySet().removeAll(tombstone.getFields());
    this.ranges = new HashMap<>();

    Map<F, RangeSet<K>> rangeSetMap = new HashMap<>();
    for (Map.Entry<F, Range<K>> entry : tableMeta.getRanges().entrySet()) {
      F field = entry.getKey();
      Range<K> range = entry.getValue();
      rangeSetMap.put(field, TreeRangeSet.create(Collections.singleton(range)));
    }
    rangeSetMap.keySet().removeAll(tombstone.getFields());
    rangeSetMap.values().forEach(rangeSet -> rangeSet.removeAll(tombstone.getKeys()));
    for (Map.Entry<F, RangeSet<K>> entry : tombstone.getSegments().entrySet()) {
      F field = entry.getKey();
      RangeSet<K> rangeSetDeleted = entry.getValue();
      RangeSet<K> rangeSet = rangeSetMap.get(field);
      if (rangeSet != null) {
        rangeSet.removeAll(rangeSetDeleted);
      }
    }
    for (Map.Entry<F, RangeSet<K>> entry : rangeSetMap.entrySet()) {
      F field = entry.getKey();
      RangeSet<K> rangeSet = entry.getValue();
      if (!rangeSet.isEmpty()) {
        ranges.put(field, rangeSet.span());
      }
    }
  }

  @Override
  public Map<F, T> getSchema() {
    return schema;
  }

  @Override
  public Map<F, Range<K>> getRanges() {
    return ranges;
  }
}
