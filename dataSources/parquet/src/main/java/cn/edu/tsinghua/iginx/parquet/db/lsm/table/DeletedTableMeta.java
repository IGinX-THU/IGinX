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
