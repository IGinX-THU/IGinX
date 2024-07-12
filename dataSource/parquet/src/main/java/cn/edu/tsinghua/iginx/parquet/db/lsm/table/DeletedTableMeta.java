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
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;

public class DeletedTableMeta implements TableMeta {
  private final Map<String, DataType> schema;
  private final Map<String, Range<Long>> ranges;
  private final Map<String, Long> counts;

  public DeletedTableMeta(TableMeta tableMeta, AreaSet<Long, String> tombstone) {
    this.schema = new HashMap<>(tableMeta.getSchema());
    schema.keySet().removeAll(tombstone.getFields());

    this.counts = new HashMap<>();

    Map<String, RangeSet<Long>> rangeSetMap = new HashMap<>();
    for (Map.Entry<String, DataType> entry : schema.entrySet()) {
      String field = entry.getKey();
      Range<Long> range = tableMeta.getRange(field);
      rangeSetMap.put(field, TreeRangeSet.create(Collections.singleton(range)));
      Long count = tableMeta.getValueCount(field);
      if (count != null) {
        counts.put(field, count);
      }
    }

    RangeSet<Long> deletedKeys = tombstone.getKeys();
    if (!deletedKeys.isEmpty()) {
      counts.clear();
      rangeSetMap.values().forEach(rangeSet -> rangeSet.removeAll(deletedKeys));
    }

    for (Map.Entry<String, RangeSet<Long>> entry : tombstone.getSegments().entrySet()) {
      String field = entry.getKey();
      RangeSet<Long> rangeSetDeleted = entry.getValue();
      RangeSet<Long> rangeSet = rangeSetMap.get(field);
      if (rangeSet != null) {
        rangeSet.removeAll(rangeSetDeleted);
      }
      counts.remove(field);
    }

    this.ranges = new HashMap<>();
    for (Map.Entry<String, RangeSet<Long>> entry : rangeSetMap.entrySet()) {
      String field = entry.getKey();
      RangeSet<Long> rangeSet = entry.getValue();
      if (!rangeSet.isEmpty()) {
        ranges.put(field, rangeSet.span());
      } else {
        ranges.put(field, Range.closedOpen(0L, 0L));
      }
    }
  }

  @Override
  public Map<String, DataType> getSchema() {
    return schema;
  }

  @Override
  public Range<Long> getRange(String field) {
    if (!schema.containsKey(field)) {
      throw new NoSuchElementException();
    }
    return ranges.get(field);
  }

  @Override
  @Nullable
  public Long getValueCount(String field) {
    if (!schema.containsKey(field)) {
      throw new NoSuchElementException();
    }
    return counts.get(field);
  }
}
