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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.db.lsm.table;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.db.lsm.api.ReadWriter;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.db.lsm.api.TableMeta;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.db.lsm.buffer.DataBuffer;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.db.util.iterator.ConcatScanner;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.db.util.iterator.EmtpyHeadRowScanner;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.db.util.iterator.RowUnionScanner;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.util.Shared;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.util.arrow.ArrowFields;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.util.exception.StorageException;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.util.exception.TypeConflictedException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.common.collect.*;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableStorage implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableStorage.class);

  private final TableIndex tableIndex;
  private final ReadWriter readWriter;
  private long sqnBase;

  public TableStorage(Shared shared, ReadWriter readWriter) throws IOException {
    this.readWriter = readWriter;

    Iterable<String> tableNames = readWriter.reload();
    String last =
        StreamSupport.stream(tableNames.spliterator(), false)
            .max(Comparator.naturalOrder())
            .orElse("0-0");
    this.sqnBase = getSeq(last) + 1;
    this.tableIndex = new TableIndex(this);
  }

  static long getSeq(String tableName) {
    Pattern pattern = Pattern.compile("^(\\d+)-.*$");
    Matcher matcher = pattern.matcher(tableName);
    if (matcher.find()) {
      return Long.parseLong(matcher.group(1));
    } else {
      throw new IllegalArgumentException("invalid table name: " + tableName);
    }
  }

  public String getTableName(long sqn, String suffix) {
    return String.format("%019d-%s", sqnBase + sqn, suffix);
  }

  public List<String> flush(long sqn, String suffix, MemoryTable table)
      throws InterruptedException {
    if (table.isEmpty()) {
      return Collections.emptyList();
    }
    String name = getTableName(sqn, suffix);
    TableMeta meta = table.getMeta();
    try (Scanner<Long, Scanner<String, Object>> scanner =
        table.scan(meta.getSchema().keySet(), ImmutableRangeSet.of(Range.all()))) {
      readWriter.flush(name, meta, scanner);
    } catch (IOException | StorageException e) {
      LOGGER.error("flush table {} failed", name, e);
    }
    return Collections.singletonList(name);
  }

  public void commit(String table, AreaSet<Long, Field> tombstone) {
    AreaSet<Long, String> innerTombstone = ArrowFields.toInnerAreas(tombstone);
    try {
      if (innerTombstone.isAll()) {
        readWriter.delete(table);
        return;
      }
      if (!innerTombstone.isEmpty()) {
        readWriter.delete(table, innerTombstone);
      }
      TableMeta meta = readWriter.readMeta(table);
      tableIndex.addTable(table, meta);
    } catch (IOException e) {
      LOGGER.error("commit table {} failed", table, e);
    }
  }

  public void clear() {
    sqnBase = 0;
    tableIndex.clear();
    try {
      readWriter.clear();
    } catch (IOException e) {
      LOGGER.error("clear failed", e);
    }
  }

  public TableMeta getMeta(String tableName) throws IOException {
    return new FileTable(tableName, readWriter).getMeta();
  }

  public void delete(AreaSet<Long, String> areas) throws IOException {
    Set<String> tables = tableIndex.find(areas);
    tableIndex.delete(areas);
    for (String tableName : tables) {
      readWriter.delete(tableName, areas);
    }
  }

  public Iterable<String> reload() throws IOException {
    return readWriter.reload();
  }

  @Override
  public void close() {}

  public Map<String, DataType> schema() {
    return tableIndex.getType();
  }

  public void declareFields(Map<String, DataType> schema) throws TypeConflictedException {
    tableIndex.declareFields(schema);
  }

  public DataBuffer<Long, String, Object> query(
      Set<String> fields, RangeSet<Long> ranges, Filter filter)
      throws StorageException, IOException {

    AreaSet<Long, String> areas = new AreaSet<>();
    areas.add(fields, ranges);
    DataBuffer<Long, String, Object> buffer = new DataBuffer<>();

    Set<String> tables = tableIndex.find(areas);
    List<String> sortedTableNames = new ArrayList<>(tables);
    sortedTableNames.sort(Comparator.naturalOrder());

    for (String tableName : sortedTableNames) {
      try (Scanner<Long, Scanner<String, Object>> scanner = scan(tableName, fields, ranges)) {
        buffer.putRows(scanner);
      }
    }

    return buffer;
  }

  private Scanner<Long, Scanner<String, Object>> scan(
      String tableName, Set<String> fields, RangeSet<Long> ranges) throws IOException {
    return new FileTable(tableName, readWriter).scan(fields, ranges);
  }

  public Map<String, Long> count(Set<String> innerFields) throws StorageException, IOException {
    Map<String, Long> counts = new HashMap<>();

    for (String field : innerFields) {
      long count = count(field);
      counts.put(field, count);
    }

    return counts;
  }

  public long count(String field) throws StorageException, IOException {
    RangeMap<Long, List<String>> regionTableLists = getTablesGroupByRegion(field);

    long totalCount = 0;
    for (List<String> tables : regionTableLists.asMapOfRanges().values()) {
      if (tables.isEmpty()) {
        continue;
      } else if (tables.size() == 1) {
        TableMeta meta = readWriter.readMeta(tables.get(0));
        Long regionCount = meta.getValueCount(field);
        if (regionCount != null) {
          totalCount += regionCount;
          continue;
        }
      }
      totalCount += getOverlapCount(field, tables);
    }
    return totalCount;
  }

  private static Range<Long> normalize(Range<Long> range) {
    if (range.isEmpty()) {
      return range;
    }
    long lower = range.lowerEndpoint();
    long upper = range.upperEndpoint();
    if (range.lowerBoundType() == BoundType.OPEN) {
      lower++;
    }
    if (range.upperBoundType() == BoundType.OPEN) {
      upper--;
    }
    return Range.closed(lower, upper);
  }

  private RangeMap<Long, List<String>> getTablesGroupByRegion(String field) throws IOException {
    AreaSet<Long, String> areas = new AreaSet<>();
    areas.add(Collections.singleton(field), ImmutableRangeSet.of(Range.all()));
    Set<String> tables = tableIndex.find(areas);
    List<String> sortedTableNames = new ArrayList<>(tables);
    sortedTableNames.sort(Comparator.naturalOrder());

    HashMap<String, Range<Long>> tableRanges = new HashMap<>();
    for (String tableName : sortedTableNames) {
      TableMeta meta = readWriter.readMeta(tableName);
      Range<Long> range = meta.getRange(field);
      tableRanges.put(tableName, range);
    }

    RangeSet<Long> regions = TreeRangeSet.create(tableRanges.values());
    RangeMap<Long, List<String>> regionTableLists = TreeRangeMap.create();
    for (Range<Long> region : regions.asRanges()) {
      regionTableLists.put(region, new ArrayList<>());
    }

    for (String tableName : sortedTableNames) {
      Range<Long> range = normalize(tableRanges.get(tableName));
      List<String> regionTableList = regionTableLists.get(range.lowerEndpoint());
      assert regionTableList != null;
      regionTableList.add(tableName);
    }

    return regionTableLists;
  }

  private long getOverlapCount(String field, List<String> sortedTableNames)
      throws IOException, StorageException {
    Set<String> fields = Collections.singleton(field);

    long count = 0;
    try (Scanner<Long, Scanner<String, Object>> scanner = scan(sortedTableNames, fields)) {
      while (scanner.iterate()) {
        Scanner<String, Object> row = scanner.value();
        while (row.iterate()) {
          assert row.value() != null;
          count++;
        }
      }
    }
    return count;
  }

  private Scanner<Long, Scanner<String, Object>> scan(List<String> tableNames, Set<String> fields)
      throws IOException, StorageException {
    List<FileTable> tables = new ArrayList<>();
    for (String tableName : tableNames) {
      tables.add(new FileTable(tableName, readWriter));
    }
    List<Scanner<Long, Scanner<String, Object>>> overlaps = getOverlapScannerList(fields, tables);

    Collections.reverse(overlaps);
    return new RowUnionScanner<>(overlaps);
  }

  private static List<Scanner<Long, Scanner<String, Object>>> getOverlapScannerList(
      Set<String> fields, List<FileTable> tables) throws IOException {
    RangeSet<Long> ranges = ImmutableRangeSet.of(Range.all());
    List<Scanner<Long, Scanner<String, Object>>> overlaps = new ArrayList<>();

    RangeSet<Long> tableRanges = TreeRangeSet.create();
    List<Scanner<Long, Scanner<String, Object>>> noOverlaps = new ArrayList<>();
    for (FileTable table : tables) {
      TableMeta meta = table.getMeta();
      Range<Long> range = normalize(meta.getRange(fields));
      if (range.isEmpty()) {
        continue;
      }
      if (tableRanges.intersects(range)) {
        overlaps.add(new ConcatScanner<>(noOverlaps.iterator()));
        noOverlaps = new ArrayList<>();
        tableRanges = TreeRangeSet.create();
      }
      long head = range.lowerEndpoint();
      Scanner<Long, Scanner<String, Object>> lazy = table.lazyScan(fields, ranges);
      Scanner<Long, Scanner<String, Object>> emptyHead = new EmtpyHeadRowScanner<>(head);
      Scanner<Long, Scanner<String, Object>> concat =
          new ConcatScanner<>(Iterators.forArray(emptyHead, lazy));
      noOverlaps.add(concat);
      tableRanges.add(range);
    }
    if (!noOverlaps.isEmpty()) {
      overlaps.add(new ConcatScanner<>(noOverlaps.iterator()));
    }

    return overlaps;
  }
}
