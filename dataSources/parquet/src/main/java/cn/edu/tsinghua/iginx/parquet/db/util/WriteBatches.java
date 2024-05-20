package cn.edu.tsinghua.iginx.parquet.db.util;

import cn.edu.tsinghua.iginx.engine.physical.storage.domain.ColumnKey;
import cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.chunk.UnorderedChunk;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.parquet.manager.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.parquet.util.arrow.ArrowFields;
import cn.edu.tsinghua.iginx.parquet.util.arrow.ArrowTypes;
import cn.edu.tsinghua.iginx.parquet.util.arrow.ArrowVectors;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.Types;

public class WriteBatches {

  private static class ChunkSnapshotBuilder {
    private final BigIntVector keyVector;
    private final ValueVector valueVector;
    private final FieldWriter valueWriter;
    private final Consumer<Object> valueAppender;
    private int count = 0;

    public ChunkSnapshotBuilder(ColumnKey columnKey, DataType type, BufferAllocator allocator) {
      this.keyVector = ArrowVectors.key(allocator);
      Types.MinorType minorType = ArrowTypes.minorTypeOf(type);
      this.valueVector =
          minorType.getNewVector(ArrowFields.of(false, columnKey, type), allocator, null);
      this.valueWriter = minorType.getNewFieldWriter(valueVector);
      switch (type) {
        case BOOLEAN:
          valueAppender = value -> valueWriter.writeBit(((Boolean) value) ? 1 : 0);
          break;
        case INTEGER:
          valueAppender = value -> valueWriter.writeInt((Integer) value);
          break;
        case LONG:
          valueAppender = value -> valueWriter.writeBigInt((Long) value);
          break;
        case FLOAT:
          valueAppender = value -> valueWriter.writeFloat4((Float) value);
          break;
        case DOUBLE:
          valueAppender = value -> valueWriter.writeFloat8((Double) value);
          break;
        case BINARY:
          valueAppender = value -> valueWriter.writeVarBinary((byte[]) value);
          break;
        default:
          throw new UnsupportedOperationException("Unsupported data type: " + type);
      }
    }

    public void append(long key, Object value) {
      keyVector.setSafe(count, key);
      valueWriter.setPosition(count);
      valueAppender.accept(value);
      count++;
    }

    public UnorderedChunk.Snapshot build() {
      keyVector.setValueCount(count);
      valueVector.setValueCount(count);
      return new UnorderedChunk.Snapshot(keyVector, valueVector);
    }
  }

  private static <F, T> Map<String, ChunkSnapshotBuilder> builders(
      Map<F, T> schema, BufferAllocator allocator) {
    Map<String, ChunkSnapshotBuilder> headers = new HashMap<>();
    for (Map.Entry<F, T> entry : schema.entrySet()) {
      String name = (String) entry.getKey();
      DataType type = (DataType) entry.getValue();
      ColumnKey columnKey = TagKVUtils.splitFullName(name);
      ChunkSnapshotBuilder builder = new ChunkSnapshotBuilder(columnKey, type, allocator);
      headers.put(name, builder);
    }
    return headers;
  }

  public static <V, F, K extends Comparable<K>, T> Iterable<UnorderedChunk.Snapshot> recordOfRows(
      Scanner<K, Scanner<F, V>> rows, Map<F, T> schema, BufferAllocator allocator)
      throws StorageException {
    Map<String, ChunkSnapshotBuilder> builders = builders(schema, allocator);
    while (rows.iterate()) {
      long key = (Long) rows.key();
      Scanner<F, V> row = rows.value();
      while (row.iterate()) {
        String name = (String) row.key();
        Object value = row.value();
        ChunkSnapshotBuilder builder = builders.get(name);
        builder.append(key, value);
      }
    }
    return builders.values().stream().map(ChunkSnapshotBuilder::build).collect(Collectors.toList());
  }

  public static <V, F, K extends Comparable<K>, T>
      Iterable<UnorderedChunk.Snapshot> recordOfColumns(
          Scanner<F, Scanner<K, V>> batch, Map<F, T> schema, BufferAllocator allocator)
          throws StorageException {
    Map<String, ChunkSnapshotBuilder> builders = builders(schema, allocator);
    while (batch.iterate()) {
      String name = (String) batch.key();
      Scanner<K, V> column = batch.value();
      ChunkSnapshotBuilder builder = builders.get(name);
      while (column.iterate()) {
        long key = (Long) column.key();
        Object value = column.value();
        builder.append(key, value);
      }
    }
    return builders.values().stream().map(ChunkSnapshotBuilder::build).collect(Collectors.toList());
  }
}
