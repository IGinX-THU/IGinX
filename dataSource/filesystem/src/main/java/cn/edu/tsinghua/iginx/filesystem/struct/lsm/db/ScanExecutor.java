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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.common.Filters;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.FilterRangeUtils;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.Table;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.event.TableReadEvent;
import com.google.common.collect.*;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Data;
import lombok.Value;
import org.apache.arrow.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScanExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScanExecutor.class);

  private final String name;
  private final Semaphore scannerPermits;
  private final ExecutorService executor = Executors.newCachedThreadPool();

  public ScanExecutor(String name, Semaphore scannerPermits) {
    this.name = name;
    this.scannerPermits = scannerPermits;
  }

  public RowStream scan(List<Table> allHitTables, List<Field> fields, Filter filter)
      throws PhysicalException, IOException {

    List<TablePlan> plans = new ArrayList<>();
    RangeSet<Long> rangeSet = FilterRangeUtils.rangeSetOf(filter);
    for (Table table : allHitTables) {
      List<SubTablePlan> subTablePlans = new ArrayList<>();
      List<Table.SubTable> subTables = table.getSubTables();
      for (Table.SubTable subTable : subTables) {
        Table.Meta meta = subTable.getMeta();
        ImmutableMap<Field, Table.Statistic> fieldStats = meta.getFieldStats();
        List<Field> hitFields =
            fields.stream().filter(fieldStats::containsKey).collect(Collectors.toList());
        RangeSet<Long> hitRangeSet = TreeRangeSet.create();
        fieldStats.entrySet().stream()
            .filter(e -> hitFields.contains(e.getKey()))
            .map(Map.Entry::getValue)
            .map(Table.Statistic::getKeyRange)
            .forEach(hitRangeSet::add);
        hitRangeSet.removeAll(rangeSet.complement());
        if (!hitFields.isEmpty() && !hitRangeSet.isEmpty()) {
          Filter defaultRangeFilter = FilterRangeUtils.filterOf(hitRangeSet);
          SubTablePlan subTablePlan =
              new SubTablePlan(subTable, hitFields, hitRangeSet, defaultRangeFilter);
          subTablePlans.add(subTablePlan);
        }
      }
      TablePlan plan = new TablePlan(table, subTablePlans);
      plans.add(plan);
    }

    analyseOverlap(plans);
    pushdownFilter(plans, fields, filter);

    ResultTableBuilder builder = new ResultTableBuilder(fields);
    executePlans(plans, builder);

    boolean allPushDown =
        plans.stream()
            .flatMap(p -> p.getSubTablePlans().stream())
            .filter(p -> p.getResultRows() > 0)
            .allMatch(st -> st.getFilter().equals(filter));
    RowStream result = builder.build();
    Filter filterWithoutRange = FilterRangeUtils.withoutKeyRangeSet(filter);
    if (!allPushDown && !Filters.isTrue(filterWithoutRange)) {
      result = new FilterRowStreamWrapper(result, filterWithoutRange);
    }
    return result;
  }

  private void executePlans(List<TablePlan> plans, ResultTableBuilder builder)
      throws PhysicalException, IOException {
    // 1. 数据分类：使用 Queue 方便弹出
    Queue<SubTablePlan> parallelQueue = new ArrayDeque<>();
    Queue<SubTablePlan> sequentialPlans = new ArrayDeque<>();
    for (TablePlan plan : plans) {
      for (SubTablePlan sub : plan.getSubTablePlans()) {
        if (!sub.isOverlap()) {
          parallelQueue.offer(sub);
        } else {
          sequentialPlans.add(sub);
        }
      }
    }

    // 2. 贪婪异步
    List<CompletableFuture<Void>> parallelFutures = new ArrayList<>();
    while (true) {
      SubTablePlan currPlan;
      if (!sequentialPlans.isEmpty()) {
        currPlan = sequentialPlans.poll();
      } else if (!parallelQueue.isEmpty()) {
        currPlan = parallelQueue.poll();
      } else {
        break;
      }
      while (!parallelQueue.isEmpty() && scannerPermits.tryAcquire()) {
        SubTablePlan parallelPlan = parallelQueue.poll();
        parallelFutures.add(
            CompletableFuture.runAsync(
                () -> {
                  try {
                    LOGGER.debug(
                        "Executing subTablePlan of {} in parallel: {}", name, parallelPlan);
                    executePlan(parallelPlan, builder);
                  } catch (PhysicalException | IOException e) {
                    throw new CompletionException(e);
                  } finally {
                    scannerPermits.release();
                  }
                },
                executor));
      }
      LOGGER.debug("Executing subTablePlan of {} in sequence: {}", name, currPlan);
      executePlan(currPlan, builder);
    }

    // 3. 统一等待
    if (!parallelFutures.isEmpty()) {
      try {
        CompletableFuture.allOf(parallelFutures.toArray(new CompletableFuture[0])).join();
      } catch (CompletionException e) {
        throw new PhysicalException(e);
      }
    }
  }

  private static void executePlan(SubTablePlan subTablePlan, ResultTableBuilder builder)
      throws PhysicalException, IOException {
    TableReadEvent event = new TableReadEvent();
    event.tableName = subTablePlan.getSubTable().toString();
    event.begin();
    try (RowStream rowStream =
        subTablePlan.getSubTable().scan(subTablePlan.getFields(), subTablePlan.getFilter())) {
      int rows = builder.put(rowStream);
      event.end();
      event.projectedSchema = rowStream.getHeader().toString();
      event.numRows = rows;
      subTablePlan.setResultRows(rows);
    } finally {
      event.commit();
    }
  }

  private static void analyseOverlap(List<TablePlan> plans) {
    Map<Field, RangeMap<Long, SubTablePlan>> fieldToRangeMap = new HashMap<>();
    for (TablePlan plan : plans) {
      for (SubTablePlan subTablePlan : plan.getSubTablePlans()) {
        subTablePlan.setOverlap(false);
        for (Field field : subTablePlan.getFields()) {
          RangeMap<Long, SubTablePlan> rangeMap =
              fieldToRangeMap.computeIfAbsent(field, f -> TreeRangeMap.create());
          for (Range<Long> range : subTablePlan.getRange().asRanges()) {
            RangeMap<Long, SubTablePlan> overlapping = rangeMap.subRangeMap(range);
            for (SubTablePlan other : overlapping.asMapOfRanges().values()) {
              subTablePlan.setOverlap(true);
              other.setOverlap(true);
            }
            rangeMap.put(range, subTablePlan);
          }
        }
      }
    }
  }

  private static void pushdownFilter(List<TablePlan> plans, List<Field> fields, Filter filter) {
    for (TablePlan plan : plans) {
      for (SubTablePlan subTablePlan : plan.getSubTablePlans()) {
        if (!subTablePlan.isOverlap()) {
          if (subTablePlan.getFields().size() == fields.size()) {
            subTablePlan.setFilter(filter);
          }
        }
      }
    }
  }

  @ThreadSafe
  private static class ResultTableBuilder {

    private final Header header;
    private final Object2IntOpenHashMap<cn.edu.tsinghua.iginx.engine.shared.data.read.Field>
        indexOfField = new Object2IntOpenHashMap<>();
    private final ConcurrentHashMap<Long, Object[]> keyToValues = new ConcurrentHashMap<>();

    public ResultTableBuilder(List<Field> fields) {
      this.header = new Header(Field.KEY, fields);
      IntStream.range(0, fields.size()).forEach(i -> indexOfField.put(header.getField(i), i));
      Preconditions.checkArgument(indexOfField.size() == fields.size());
    }

    public int put(RowStream rowStream) throws PhysicalException {
      Header header = rowStream.getHeader();
      Preconditions.checkArgument(header.hasKey());
      int[] dstIndex = new int[header.getFieldSize()];
      for (int i = 0; i < dstIndex.length; i++) {
        Preconditions.checkArgument(indexOfField.containsKey(header.getField(i)));
        dstIndex[i] = indexOfField.getInt(header.getField(i));
      }
      int count = 0;
      while (rowStream.hasNext()) {
        Row row = rowStream.next();
        long key = row.getKey();
        Object[] values = row.getValues();
        Object[] dstValues = keyToValues.computeIfAbsent(key, k -> new Object[indexOfField.size()]);
        for (int i = 0; i < dstIndex.length; i++) {
          Object value = values[i];
          if (value != null) {
            dstValues[dstIndex[i]] = value;
          }
        }
        count++;
      }
      return count;
    }

    public cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table build() {
      List<Row> rows =
          keyToValues.entrySet().stream()
              .sorted(Comparator.comparingLong(Map.Entry::getKey))
              .map(e -> new Row(header, e.getKey(), e.getValue()))
              .collect(Collectors.toList());
      keyToValues.clear();
      return new cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table(header, rows);
    }
  }

  @Value
  private static class TablePlan {
    @lombok.NonNull Table table;
    @lombok.NonNull List<SubTablePlan> subTablePlans;
  }

  @Data
  private static class SubTablePlan {
    @lombok.NonNull Table.SubTable subTable;
    @lombok.NonNull List<Field> fields;
    @lombok.NonNull RangeSet<Long> range;
    @lombok.NonNull Filter filter;
    boolean overlap = true;
    int resultRows = 0;
  }
}
