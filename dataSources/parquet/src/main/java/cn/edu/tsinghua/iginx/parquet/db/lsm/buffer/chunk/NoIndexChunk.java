package cn.edu.tsinghua.iginx.parquet.db.lsm.buffer.chunk;

import cn.edu.tsinghua.iginx.parquet.util.arrow.ArrowVectors;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.WillCloseWhenClosed;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.reader.BigIntReader;

@ThreadSafe
public class NoIndexChunk extends IndexedChunk {

  private final TreeMap<Integer, RangeSet<Long>> tombstone = new TreeMap<>();

  private int valueCount = 0;

  public NoIndexChunk(@WillCloseWhenClosed UnorderedChunk chunk, BufferAllocator allocator) {
    super(chunk, allocator);
    tombstone.put(0, TreeRangeSet.create());
  }

  @Override
  protected IntVector indexOf(Snapshot snapshot, BufferAllocator allocator) {
    IntVector indexes = ArrowVectors.stableSortIndexes(snapshot.keys, allocator);
    ArrowVectors.dedupSortedIndexes(snapshot.keys, indexes);
    BigIntReader reader = snapshot.getKeyReader();
    ArrowVectors.filter(indexes, i -> !isDeleted(reader, i));
    return indexes;
  }

  private boolean isDeleted(BigIntReader reader, int index) {
    Map.Entry<Integer, RangeSet<Long>> entry = tombstone.higherEntry(index);
    if (entry == null) {
      return false;
    }
    RangeSet<Long> deleted = entry.getValue();
    reader.setPosition(index);
    long key = reader.readLong();
    return deleted.contains(key);
  }

  @Override
  protected void updateIndex(Snapshot data, int offset) {
    valueCount = offset + data.getValueCount();
  }

  @Override
  protected void deleteIndex(RangeSet<Long> rangeSet) {
    tombstone.computeIfAbsent(valueCount, k -> TreeRangeSet.create()).addAll(rangeSet);
    tombstone.values().forEach(r -> r.removeAll(rangeSet));
  }
}
