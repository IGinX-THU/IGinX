package cn.edu.tsinghua.iginx.parquet.db.lsm.buffer;

import cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.chunk.Chunk;
import cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.chunk.IndexedChunk;
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
  private final IndexedChunk.Factory factory;
  private final BufferAllocator allocator;
  private final int maxChunkValueCount;
  private final int minChunkValueCount;

  public MemTable(
      IndexedChunk.Factory factory,
      BufferAllocator allocator,
      int maxChunkValueCount,
      int minChunkValueCount) {
    this.factory = factory;
    this.allocator = allocator;
    this.maxChunkValueCount = maxChunkValueCount;
    this.minChunkValueCount = minChunkValueCount;
  }

  @Override
  public void close() {
    columns.values().forEach(MemColumn::close);
    columns.clear();
  }

  public Set<Field> getFields() {
    return Collections.unmodifiableSet(columns.keySet());
  }

  public MemoryTable snapshot(
      List<Field> fields, RangeSet<Long> ranges, BufferAllocator allocator) {
    LinkedHashMap<Field, MemColumn.Snapshot> columns = new LinkedHashMap<>();
    for (Field field : fields) {
      this.columns.computeIfPresent(
          field,
          (key, column) -> {
            columns.put(field, column.snapshot(ranges, allocator));
            return column;
          });
    }
    return new MemoryTable(columns);
  }

  public MemoryTable snapshot(BufferAllocator allocator) {
    LinkedHashMap<Field, MemColumn.Snapshot> columns = new LinkedHashMap<>();
    this.columns.forEach(
        (key, column) -> {
          columns.put(key, column.snapshot(allocator));
        });
    return new MemoryTable(columns);
  }

  public void store(Iterable<Chunk.Snapshot> data) {
    data.forEach(this::store);
  }

  public void store(Chunk.Snapshot data) {
    columns.compute(
        ArrowFields.nullable(data.getField()),
        (field, column) -> {
          if (column == null) {
            column = new MemColumn(factory, allocator, maxChunkValueCount, minChunkValueCount);
          }
          column.store(data);
          return column;
        });
  }

  public void compact() {
    columns.values().forEach(MemColumn::compact);
  }

  public void delete(AreaSet<Long, Field> areas) {
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
  }
}
