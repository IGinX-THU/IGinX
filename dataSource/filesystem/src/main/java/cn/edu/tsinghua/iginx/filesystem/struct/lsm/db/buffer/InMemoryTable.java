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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer;

import cn.edu.tsinghua.iginx.engine.shared.data.read.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filesystem.common.Filters;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.AbstractTable;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.FilterRangeUtils;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.NoexceptAutoCloseable;
import com.google.common.collect.*;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntHeapPriorityQueue;
import it.unimi.dsi.fastutil.ints.IntPriorityQueue;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.WillCloseWhenClosed;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class InMemoryTable extends AbstractTable implements NoexceptAutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryTable.class);

  private final long id;
  private final MemTable.Snapshot snapshot;
  private final ImmutableList<SubTable> subTables;

  public InMemoryTable(long id, @WillCloseWhenClosed MemTable.Snapshot snapshot) {
    this.id = id;
    this.snapshot = snapshot;
    this.subTables =
        IntStream.range(0, snapshot.getSubTableCount())
            .mapToObj(snapshot::getSubTable)
            .map(InMemorySubTable::new)
            .collect(ImmutableList.toImmutableList());
  }

  @Override
  public void close() {
    snapshot.close();
  }

  @Override
  public List<SubTable> getSubTables() {
    return subTables;
  }

  @Override
  public String toString() {
    return "InMemoryTable{" +  "id=" + id + '}';
  }

  private class InMemorySubTable implements SubTable {

    private final MemSubTable.Snapshot snapshot;
    private final Meta meta;

    InMemorySubTable(MemSubTable.Snapshot snapshot) {
      this.snapshot = snapshot;
      RangeSet<Long> rangeSet = TreeRangeSet.create();
      for (MemSubTable.SortedChunkSnapshot chunk : snapshot.getChunks()) {
        rangeSet.add(chunk.getKeyRange());
      }
      Range<Long> range = rangeSet.isEmpty() ? Range.closedOpen(0L, 0L) : rangeSet.span();
      ImmutableMap<Field, Statistic> fieldStats =
          snapshot.getFields().stream().map(ArrowFields::toIginxField)
              .collect(ImmutableMap.toImmutableMap(field -> field, field -> new Statistic(range)));
      this.meta = new Meta(fieldStats);
    }

    @Override
    public String toString() {
      return "InMemorySubTable{" +
              "table=" + InMemoryTable.this +
              ", meta=" + meta +
              '}';
    }

    @Override
    public Meta getMeta() {
      return meta;
    }

    @Override
    public RowStream scan(List<Field> fields, Filter predicate) {
      RangeSet<Long> keyRangeSet = FilterRangeUtils.rangeSetOf(predicate);
      Filter remainingFilter = FilterRangeUtils.withoutKeyRangeSet(predicate);

      RowStream rowStream = new MergedRowStream(snapshot, fields, keyRangeSet);
      if (!Filters.isTrue(remainingFilter)) {
        rowStream = new FilterRowStreamWrapper(rowStream, remainingFilter);
      }
      return rowStream;
    }
  }

  /**
   * 多路归并RowStream实现 使用最小堆对多个已排序的chunk进行归并去重
   */
  private static class MergedRowStream implements RowStream {

    private final Header header;
    private final int[] fieldIndexMapping;
    private final ChunkIterator[] iterators;
    private final MergeCursor cursor;
    private Row nextRow;

    MergedRowStream(
            MemSubTable.Snapshot snapshot, List<Field> fields, RangeSet<Long> keyRangeSet) {
      this.header = new Header(Field.KEY, fields);
      this.fieldIndexMapping = buildFieldIndexMapping(snapshot, fields);
      this.iterators = collectEligibleIterators(snapshot, keyRangeSet);
      this.cursor = new MergeCursor(new IntHeapPriorityQueue(iterators.length, this::compareIterators));
      for (int i = 0; i < iterators.length; i++) {
        if (iterators[i].advance()) {
          cursor.enqueue(i);
        }
      }
      advance();
    }

    private static int[] buildFieldIndexMapping(MemSubTable.Snapshot snapshot, List<Field> fields) {
      List<Field> snapshotFields = snapshot.getFields().stream().map(ArrowFields::toIginxField).collect(Collectors.toList());

      Map<Field, Integer> snapshotFieldIndexMap = new HashMap<>();
      for (int i = 0; i < snapshotFields.size(); i++) {
        snapshotFieldIndexMap.put(snapshotFields.get(i), i);
      }
      int[] fieldIndexMapping = new int[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        Field requestedField = fields.get(i);
        Integer fieldIndex = snapshotFieldIndexMap.get(requestedField);
        if (fieldIndex == null) {
          throw new IllegalArgumentException("Requested field not found: " + requestedField);
        }
        fieldIndexMapping[i] = fieldIndex;
      }
      return fieldIndexMapping;
    }

    private static ChunkIterator[] collectEligibleIterators(
            MemSubTable.Snapshot snapshot, RangeSet<Long> keyRangeSet) {
      List<ChunkIterator> validIterators = new ArrayList<>();
      for (MemSubTable.SortedChunkSnapshot chunk : snapshot.getChunks()) {
        if (keyRangeSet.intersects(chunk.getKeyRange())) {
          validIterators.add(new ChunkIterator(chunk, keyRangeSet));
        }
      }
      return validIterators.toArray(new ChunkIterator[0]);
    }

    private int compareIterators(int i1, int i2) {
      int keyCompare = Long.compare(iterators[i1].currentKey, iterators[i2].currentKey);
      if (keyCompare != 0) {
        return keyCompare;
      }
      // key 相同场景使用固定顺序，保证行为稳定
      return Integer.compare(i1, i2);
    }

    private void advance() {
      if (cursor.isEmpty()) {
        nextRow = null;
        return;
      }

      long winnerKey = iterators[cursor.firstInt()].currentKey;
      Object[] rowValues = new Object[fieldIndexMapping.length];

      while (!cursor.isEmpty() && iterators[cursor.firstInt()].currentKey == winnerKey) {
        int duplicateIndex = cursor.dequeueInt();
        iterators[duplicateIndex].fillValues(rowValues, fieldIndexMapping);
        if (iterators[duplicateIndex].advance()) {
          cursor.enqueue(duplicateIndex);
        }
      }

      nextRow = new Row(header, winnerKey, rowValues);
    }

    @Override
    public Header getHeader() {
      return header;
    }

    @Override
    public boolean hasNext() {
      return nextRow != null;
    }

    @Override
    public Row next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Row row = nextRow;
      advance();
      return row;
    }

    @Override
    public void close() {
    }

    private static class ChunkIterator {
      private final BigIntVector keyVector;
      private final FieldVector[] fieldVectors;
      private final short[] sortedIndex;
      private final RangeSet<Long> keyRangeSet;
      private final boolean scanAllKeys;

      private int position;
      long currentKey;

      ChunkIterator(MemSubTable.SortedChunkSnapshot chunk, RangeSet<Long> keyRangeSet) {
        this.keyVector = chunk.getSnapshot().getKeyVector();
        this.fieldVectors = chunk.getSnapshot().getFieldVectors().toArray(new FieldVector[0]);
        this.sortedIndex = chunk.getIndex();
        this.keyRangeSet = keyRangeSet;
        this.scanAllKeys = keyRangeSet.encloses(chunk.getKeyRange());
        this.position = 0;
        this.currentKey = Long.MAX_VALUE;
      }

      boolean advance() {
        return scanAllKeys ? advanceWithoutRangeCheck() : advanceWithRangeCheck();
      }

      private boolean advanceWithoutRangeCheck() {
        if (position >= sortedIndex.length) {
          currentKey = Long.MAX_VALUE;
          return false;
        }
        int rowIndex = sortedIndex[position++];
        currentKey = keyVector.get(rowIndex);
        return true;
      }

      private boolean advanceWithRangeCheck() {
        while (position < sortedIndex.length) {
          int idx = sortedIndex[position++];
          long key = keyVector.get(idx);
          if (keyRangeSet.contains(key)) {
            currentKey = key;
            return true;
          }
        }
        currentKey = Long.MAX_VALUE;
        return false;
      }

      void fillValues(Object[] orderedValues, int[] fieldIndexMapping) {
        int idx = sortedIndex[position - 1];
        for (int i = 0; i < fieldIndexMapping.length; i++) {
          Object value = fieldVectors[fieldIndexMapping[i]].getObject(idx);
          if (value != null) {
            orderedValues[i] = value;
          }
        }
      }
    }

    // MergeCursor维护当前活跃的最小元素和一个堆，堆中元素都大于活跃元素
    private static final class MergeCursor implements IntPriorityQueue {
      private final IntPriorityQueue heap;
      private final IntComparator comparator;
      private int active = -1;

      MergeCursor(IntPriorityQueue heap) {
        this.heap = Objects.requireNonNull(heap);
        this.comparator = heap.comparator() == null ? Integer::compare : heap.comparator();
      }

      @Override
      public void enqueue(int x) {
        if (active < 0 && heap.isEmpty()) {
          active = x;
          return;
        }

        if (comparator.compare(x, firstInt()) > 0) {
          heap.enqueue(x);
          return;
        }

        if (active >= 0) {
          heap.enqueue(active);
        }
        active = x;
      }

      @Override
      public int dequeueInt() {
        if (active >= 0) {
          int result = active;
          active = -1;
          return result;
        }
        if (!heap.isEmpty()) {
          return heap.dequeueInt();
        }
        throw new NoSuchElementException();
      }

      @Override
      public int firstInt() {
        if (active >= 0) {
          return active;
        }
        if (!heap.isEmpty()) {
          return heap.firstInt();
        }
        throw new NoSuchElementException();
      }

      @Override
      public IntComparator comparator() {
        return comparator;
      }

      @Override
      public int size() {
        return heap.size() + (active >= 0 ? 1 : 0);
      }

      @Override
      public void clear() {
        heap.clear();
        active = -1;
      }

      @Override
      public boolean isEmpty() {
        return active < 0 && heap.isEmpty();
      }
    }
  }
}
