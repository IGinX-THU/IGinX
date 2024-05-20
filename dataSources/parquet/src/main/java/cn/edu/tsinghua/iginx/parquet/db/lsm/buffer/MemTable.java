package cn.edu.tsinghua.iginx.parquet.db.lsm.buffer;

import cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.chunk.IndexedChunk;
import cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.chunk.UnorderedChunk;
import cn.edu.tsinghua.iginx.parquet.db.lsm.table.MemoryTable;
import cn.edu.tsinghua.iginx.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.parquet.util.arrow.ArrowFields;
import com.google.common.collect.RangeSet;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Field;

@ThreadSafe
public class MemTable implements AutoCloseable {

  private final ConcurrentHashMap<Field, MemColumn> columns = new ConcurrentHashMap<>();
  private final long tableId;
  private final IndexedChunk.Factory factory;
  private final BufferAllocator allocator;
  private final int maxChunkValueCount;
  private final long maxMemorySize;
  private final long timeoutMillis;
  private volatile long lastAccessTime;

  public MemTable(
      long tableId,
      IndexedChunk.Factory factory,
      BufferAllocator allocator,
      int maxChunkValueCount,
      long maxMemorySize,
      long timeoutMillis) {
    this.tableId = tableId;
    this.factory = factory;
    String allocatorName =
        String.join(
            "/", allocator.getName(), MemTable.class.getSimpleName(), String.valueOf(tableId));
    this.allocator = allocator.newChildAllocator(allocatorName, 0, Long.MAX_VALUE);
    this.maxChunkValueCount = maxChunkValueCount;
    this.maxMemorySize = maxMemorySize;
    this.timeoutMillis = timeoutMillis;
    this.lastAccessTime = Long.MAX_VALUE;
  }

  @Override
  public void close() {
    columns.values().forEach(MemColumn::close);
    columns.clear();
    allocator.close();
  }

  public Set<Field> getFields() {
    touch();
    return Collections.unmodifiableSet(columns.keySet());
  }

  public MemoryTable snapshot(
      List<Field> fields, RangeSet<Long> ranges, BufferAllocator allocator) {
    LinkedHashMap<Field, MemColumn.Snapshot> columns = new LinkedHashMap<>();
    for (Field field : fields) {
      touch();
      this.columns.computeIfPresent(
          field,
          (key, column) -> {
            columns.put(field, column.snapshot(ranges, allocator));
            return column;
          });
      touch();
    }
    return new MemoryTable(columns);
  }

  public void store(Iterable<UnorderedChunk.Snapshot> data) {
    data.forEach(this::store);
  }

  public void store(UnorderedChunk.Snapshot data) {
    touch();
    columns.compute(
        ArrowFields.nullable(data.getField()),
        (field, column) -> {
          if (column == null) {
            column = new MemColumn(factory, allocator, maxChunkValueCount);
          }
          column.store(data);
          return column;
        });
    touch();
  }

  public void delete(AreaSet<Long, Field> areas) {
    touch();
    for (Field field : areas.getFields()) {
      MemColumn column = columns.remove(field);
      if (column != null) {
        column.close();
      }
    }
    RangeSet<Long> keys = areas.getKeys();
    if (!keys.isEmpty()) {
      columns.values().forEach(column -> column.delete(keys));
    }
    areas
        .getSegments()
        .forEach(
            (field, ranges) -> {
              columns.computeIfPresent(
                  field,
                  (key, column) -> {
                    column.delete(ranges);
                    return column;
                  });
            });
    touch();
  }

  public void touch() {
    lastAccessTime = System.currentTimeMillis();
  }

  public void setUntouched() {
    lastAccessTime = Long.MAX_VALUE;
  }

  public boolean isOverloaded() {
    return allocator.getAllocatedMemory() > maxMemorySize;
  }

  public boolean isExpired() {
    return Math.max(System.currentTimeMillis() - lastAccessTime, 1) > timeoutMillis;
  }

  public long getId() {
    return tableId;
  }
}
