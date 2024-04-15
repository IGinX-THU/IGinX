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
