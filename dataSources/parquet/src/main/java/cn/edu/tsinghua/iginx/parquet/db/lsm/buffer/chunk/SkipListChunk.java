package cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.chunk;

import cn.edu.tsinghua.iginx.parquet.util.arrow.ArrowVectors;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import javax.annotation.WillCloseWhenClosed;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.reader.BigIntReader;

@ThreadSafe
public class SkipListChunk extends IndexedChunk {

  protected final ConcurrentSkipListMap<Long, Integer> index = new ConcurrentSkipListMap<>();

  public SkipListChunk(@WillCloseWhenClosed UnorderedChunk chunk, BufferAllocator allocator) {
    super(chunk, allocator);
    try (Snapshot snapshot = chunk.snapshot(allocator)) {
      updateIndex(snapshot, 0);
    }
  }

  @Override
  protected IntVector indexOf(Snapshot snapshot, BufferAllocator allocator) {
    IntVector indexes = new IntVector("indexes", allocator);
    ArrowVectors.collect(indexes, index.values());
    return indexes;
  }

  @Override
  protected void updateIndex(Snapshot snapshot, int offset) {
    BigIntReader reader = snapshot.getKeyReader();
    for (int i = 0; i < snapshot.getValueCount(); i++) {
      reader.setPosition(i);
      index.put(reader.readLong(), i + offset);
    }
  }

  @Override
  protected void deleteIndex(RangeSet<Long> rangeSet) {
    rangeSet
        .asRanges()
        .forEach(
            range -> {
              subMapRefOf(index, range).clear();
            });
  }

  private static <K extends Comparable<K>, V> NavigableMap<K, V> subMapRefOf(
      NavigableMap<K, V> column, Range<K> range) {
    if (range.encloses(Range.all())) {
      return column;
    } else if (!range.hasLowerBound()) {
      return column.headMap(range.upperEndpoint(), range.upperBoundType() == BoundType.CLOSED);
    } else if (!range.hasUpperBound()) {
      return column.tailMap(range.lowerEndpoint(), range.lowerBoundType() == BoundType.CLOSED);
    } else {
      return column.subMap(
          range.lowerEndpoint(),
          range.lowerBoundType() == BoundType.CLOSED,
          range.upperEndpoint(),
          range.upperBoundType() == BoundType.CLOSED);
    }
  }

  @Override
  public void close() {
    super.close();
    index.clear();
  }
}
