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

import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.NoexceptAutoCloseable;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntImmutableList;
import it.unimi.dsi.fastutil.ints.IntLinkedOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectObjectImmutablePair;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.WillCloseWhenClosed;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;

@ThreadSafe
public class MemTable implements NoexceptAutoCloseable {

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private final int maxChunkValueCount;
  private final BufferAllocator allocator;
  private final Map<List<Field>, MemSubTable> subTables = new IdentityHashMap<>();
  private final Map<Field, List<Field>> fieldToSchema = new HashMap<>();

  public MemTable(BufferAllocator allocator, int maxChunkValueCount) {
    this.allocator = allocator;
    this.maxChunkValueCount = maxChunkValueCount;
  }

  public Snapshot snapshot(BufferAllocator allocator) {
    lock.readLock().lock();
    try {
      return snapshot(ImmutableList.copyOf(fieldToSchema.keySet()), allocator);
    } finally {
      lock.readLock().unlock();
    }
  }

  public Snapshot snapshot(List<Field> mayBeExistedFields, BufferAllocator allocator) {
    lock.readLock().lock();
    try {
      List<MemSubTable.Snapshot> subTableSnapshots = new ArrayList<>();
      List<IntImmutableList> subTableFieldIndex = new ArrayList<>();

      Map<List<Field>, Pair<int[], int[]>> schemaToSourceTargetIndex =
          hitSubTable(mayBeExistedFields);
      for (Map.Entry<List<Field>, Pair<int[], int[]>> entry :
          schemaToSourceTargetIndex.entrySet()) {
        List<Field> schema = entry.getKey();
        int[] sourceIndexes = entry.getValue().left();
        int[] targetIndexes = entry.getValue().right();
        MemSubTable subTable = subTables.get(schema);
        if (subTable == null) {
          continue;
        }
        subTableSnapshots.add(subTable.snapshot(targetIndexes, allocator));
        subTableFieldIndex.add(IntImmutableList.of(sourceIndexes));
      }

      return new Snapshot(subTableSnapshots, subTableFieldIndex);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void append(MemBatch.Snapshot data) {
    List<Field> fields =
        data.getFieldVectors().stream()
            .map(FieldVector::getField)
            .distinct()
            .collect(ImmutableList.toImmutableList());

    lock.readLock().lock();
    try {
      boolean needSplitOrCreate = false;
      Map<List<Field>, Pair<int[], int[]>> schemaToSourceTargetIndex = hitSubTable(fields);
      for (Map.Entry<List<Field>, Pair<int[], int[]>> entry :
          schemaToSourceTargetIndex.entrySet()) {
        List<Field> schema = entry.getKey();
        int[] sourceIndexes = entry.getValue().left();
        if (!subTables.containsKey(schema)) {
          // need create
          needSplitOrCreate = true;
          break;
        }
        if (sourceIndexes.length != schema.size()) {
          // need split
          needSplitOrCreate = true;
          break;
        }
      }
      if (!needSplitOrCreate) {
        doAppend(data, schemaToSourceTargetIndex);
        return;
      }
    } finally {
      lock.readLock().unlock();
    }

    lock.writeLock().lock();
    try {
      doAppend(data, hitSubTable(fields));
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void doAppend(
      MemBatch.Snapshot data, Map<List<Field>, Pair<int[], int[]>> schemaToSourceTargetIndex) {
    for (Map.Entry<List<Field>, Pair<int[], int[]>> entry : schemaToSourceTargetIndex.entrySet()) {
      List<Field> schema = entry.getKey();
      int[] sourceIndexes = entry.getValue().left();
      int[] targetIndexes = entry.getValue().right();

      try (MemBatch.Snapshot dataSlice = data.slice(sourceIndexes, allocator)) {
        MemSubTable subTable;
        if (!subTables.containsKey(schema)) {
          // create new sub-table
          subTable = new MemSubTable(ImmutableList.copyOf(schema), allocator, maxChunkValueCount);
          addSubTable(subTable);
        } else if (sourceIndexes.length == schema.size()) {
          // just append
          subTable = subTables.get(schema);
        } else {
          // spilt and append
          schema.forEach(fieldToSchema::remove);
          MemSubTable oldSubTable = subTables.remove(schema);
          subTable = oldSubTable.split(IntLinkedOpenHashSet.of(targetIndexes), allocator);
          addSubTable(oldSubTable);
          addSubTable(subTable);
        }
        subTable.append(dataSlice);
      }
    }
  }

  private void addSubTable(MemSubTable subTable) {
    List<Field> schema = subTable.getFields();
    subTables.put(schema, subTable);
    for (Field field : schema) {
      List<Field> oldValue = fieldToSchema.put(field, schema);
      Preconditions.checkState(oldValue == null);
    }
  }

  private Map<List<Field>, Pair<int[], int[]>> hitSubTable(List<Field> fields) {
    Map<List<Field>, IntList> schemaToHit = new IdentityHashMap<>();
    for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
      Field mayBeExistedField = fields.get(fieldIndex);
      schemaToHit
          .computeIfAbsent(fieldToSchema.get(mayBeExistedField), key -> new IntArrayList())
          .add(fieldIndex);
    }

    Map<List<Field>, Pair<int[], int[]>> hitSourceTargetIndex = new IdentityHashMap<>();
    for (Map.Entry<List<Field>, IntList> entry : schemaToHit.entrySet()) {
      int[] sourceIndexes = entry.getValue().toIntArray();
      List<Field> schema = entry.getKey();
      if (schema == null) {
        List<Field> newFields =
            Arrays.stream(sourceIndexes)
                .boxed()
                .map(fields::get)
                .collect(ImmutableList.toImmutableList());
        int[] targetIndexes = IntStream.range(0, newFields.size()).toArray();
        hitSourceTargetIndex.put(
            newFields, ObjectObjectImmutablePair.of(sourceIndexes, targetIndexes));
      } else {
        Map<Field, Integer> schemaField2Index =
            IntStream.range(0, schema.size())
                .boxed()
                .collect(Collectors.toMap(schema::get, Integer::valueOf));
        int[] targetIndexes =
            Arrays.stream(sourceIndexes)
                .mapToObj(fields::get)
                .map(schemaField2Index::get)
                .mapToInt(Integer::intValue)
                .toArray();
        hitSourceTargetIndex.put(
            schema, ObjectObjectImmutablePair.of(sourceIndexes, targetIndexes));
      }
    }
    return hitSourceTargetIndex;
  }

  @Override
  public void close() {
    lock.writeLock().lock();
    try {
      subTables.values().forEach(MemSubTable::close);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Immutable
  public static class Snapshot implements NoexceptAutoCloseable {
    private final List<MemSubTable.Snapshot> subTables;
    private final List<IntImmutableList> subTableFieldIndex;

    Snapshot(
        @WillCloseWhenClosed List<MemSubTable.Snapshot> subTables,
        List<IntImmutableList> subTableFieldIndex) {
      this.subTables = subTables;
      this.subTableFieldIndex = subTableFieldIndex;
    }

    public static Snapshot empty() {
      return new Snapshot(Collections.emptyList(), Collections.emptyList());
    }

    public int getSubTableCount() {
      return subTables.size();
    }

    public MemSubTable.Snapshot getSubTable(int index) {
      return subTables.get(index);
    }

    public IntList getFieldIndex(int index) {
      return subTableFieldIndex.get(index);
    }

    @Override
    public void close() {
      subTables.forEach(MemSubTable.Snapshot::close);
    }
  }
}
