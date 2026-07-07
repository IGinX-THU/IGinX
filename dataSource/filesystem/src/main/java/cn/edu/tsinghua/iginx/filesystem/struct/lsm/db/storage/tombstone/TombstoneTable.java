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

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.common.Filters;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.AbstractTable;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.FilterRangeUtils;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TombstoneTable extends AbstractTable {

  private final Table table;

  private final Tombstone tombstone;

  public TombstoneTable(Table table, Tombstone tombstone) {
    this.table = table;
    this.tombstone = tombstone;
  }

  @Override
  public List<SubTable> getSubTables() throws IOException {
    return table.getSubTables().stream()
        .map(t -> new TombstoneSubTable(t, tombstone))
        .collect(ImmutableList.toImmutableList());
  }

  @Override
  public String toString() {
    return "TombstoneTable{" + "table=" + table + ", tombstone=" + tombstone + '}';
  }

  private static class TombstoneSubTable implements SubTable {
    private final SubTable subTable;
    private final Tombstone tombstone;

    public TombstoneSubTable(SubTable subTable, Tombstone tombstone) {
      this.subTable = subTable;
      this.tombstone = tombstone;
    }

    @Override
    public String toString() {
      return "TombstoneSubTable{" + "subTable=" + subTable + ", tombstone=" + tombstone + '}';
    }

    @Override
    public Meta getMeta() throws IOException {
      Meta meta = subTable.getMeta();
      ImmutableMap.Builder<Field, Statistic> fieldBuilder = ImmutableMap.builder();
      for (Map.Entry<Field, Statistic> entry : meta.getFieldStats().entrySet()) {
        Field field = entry.getKey();
        Statistic statistic = entry.getValue();
        if (tombstone.getKeyRanges().containsKey(field)) {
          RangeSet<Long> keyRangeSet = tombstone.getKeyRanges().get(field);
          if (keyRangeSet == null) {
            continue;
          }
          Range<Long> keyRange = statistic.getKeyRange();
          RangeSet<Long> deletedKeyRangeSet = keyRangeSet.complement().subRangeSet(keyRange);
          Range<Long> deletedKeyRange =
              deletedKeyRangeSet.isEmpty() ? Range.closedOpen(0L, 0L) : deletedKeyRangeSet.span();
          fieldBuilder.put(field, new Statistic(deletedKeyRange));
        } else {
          fieldBuilder.put(field, statistic);
        }
      }
      return new Meta(fieldBuilder.build());
    }

    @Override
    public RowStream scan(List<Field> fields, Filter predicate) throws IOException {
      List<RangeSet<Long>> tombstoneKeyRangeSets = new ArrayList<>();
      for (Field field : fields) {
        if (tombstone.getKeyRanges().containsKey(field)) {
          RangeSet<Long> keyRangeSet = tombstone.getKeyRanges().get(field);
          if (keyRangeSet == null) {
            throw new IllegalArgumentException("Requested field not found: " + field);
          }
          tombstoneKeyRangeSets.add(keyRangeSet);
        } else {
          tombstoneKeyRangeSets.add(null);
        }
      }

      RangeSet<Long> rangeSet = FilterRangeUtils.rangeSetOf(predicate);
      Filter rangeFilter = FilterRangeUtils.filterOf(rangeSet);

      RowStream rowStream = subTable.scan(fields, rangeFilter);
      rowStream = new TombstoneRowStream(rowStream, tombstoneKeyRangeSets);
      rowStream = new ClearEmptyRowStreamWrapper(rowStream);

      Filter predicateWithoutRange = FilterRangeUtils.withoutKeyRangeSet(predicate);
      if (!Filters.isTrue(predicateWithoutRange)) {
        rowStream = new FilterRowStreamWrapper(rowStream, predicateWithoutRange);
      }
      return rowStream;
    }
  }

  private static class TombstoneRowStream implements RowStream {
    private final RowStream rowStream;
    private final List<RangeSet<Long>> tombstoneKeyRangeSets;

    public TombstoneRowStream(RowStream rowStream, List<RangeSet<Long>> tombstoneKeyRangeSets) {
      this.rowStream = rowStream;
      this.tombstoneKeyRangeSets = tombstoneKeyRangeSets;
    }

    @Override
    public Header getHeader() throws PhysicalException {
      return rowStream.getHeader();
    }

    @Override
    public boolean hasNext() throws PhysicalException {
      return rowStream.hasNext();
    }

    @Override
    public Row next() throws PhysicalException {
      Row nextRow = rowStream.next();
      Object[] oldValues = nextRow.getValues();
      Object[] values = new Object[oldValues.length];
      long key = nextRow.getKey();
      for (int i = 0; i < values.length; i++) {
        if (tombstoneKeyRangeSets.get(i) == null || !tombstoneKeyRangeSets.get(i).contains(key)) {
          values[i] = oldValues[i];
        }
      }
      return new Row(nextRow.getHeader(), key, values);
    }

    @Override
    public void close() throws PhysicalException {
      rowStream.close();
    }
  }
}
