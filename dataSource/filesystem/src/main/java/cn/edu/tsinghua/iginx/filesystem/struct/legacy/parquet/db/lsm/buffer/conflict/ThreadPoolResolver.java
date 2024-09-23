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
package cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.lsm.buffer.conflict;

import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.lsm.buffer.MemTable;
import cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.lsm.buffer.chunk.Chunk;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import org.apache.arrow.vector.types.pojo.Field;

public class ThreadPoolResolver implements ConflictResolver {
  private final ConcurrentMap<Field, ExecutorService> executors = new ConcurrentHashMap<>();
  private final Set<Field> touched = Collections.newSetFromMap(new ConcurrentHashMap<>());

  @Override
  public void reset() {
    // remove all untouched fields
    executors.keySet().removeIf(field -> !touched.contains(field));
    touched.clear();
  }

  @Override
  public void append(MemTable activeTable, Iterable<Chunk.Snapshot> data) {
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (Chunk.Snapshot chunk : data) {
      CompletableFuture<Void> future =
          CompletableFuture.runAsync(
              () -> {
                activeTable.store(chunk);
              },
              getExecutor(chunk));
      futures.add(future);
    }
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
  }

  private ExecutorService getExecutor(Field field) {
    touched.add(field);
    return executors.computeIfAbsent(field, f -> Executors.newSingleThreadExecutor());
  }

  private ExecutorService getExecutor(Chunk.Snapshot data) {
    return getExecutor(data.getField());
  }
}
