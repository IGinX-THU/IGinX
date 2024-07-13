package cn.edu.tsinghua.iginx.parquet.db.lsm.buffer;

import cn.edu.tsinghua.iginx.parquet.db.lsm.table.MemoryTable;
import cn.edu.tsinghua.iginx.parquet.db.util.AreaSet;
import cn.edu.tsinghua.iginx.parquet.util.NoexceptAutoCloseable;
import com.google.common.collect.RangeSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import javax.annotation.WillCloseWhenClosed;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.Field;

public class ArchivedMemTable implements NoexceptAutoCloseable {
  private final MemTable memTable;
  private final Collection<NoexceptAutoCloseable> onClose;
  private final AreaSet<Long, Field> deleted = new AreaSet<>();
  private final CountDownLatch latch = new CountDownLatch(1);
  private boolean snapshot = false;

  public ArchivedMemTable(
      @WillCloseWhenClosed MemTable memTable,
      @WillCloseWhenClosed Collection<NoexceptAutoCloseable> onClose) {
    this.memTable = Preconditions.checkNotNull(memTable);
    this.onClose = new ArrayList<>(onClose);
  }

  public synchronized MemoryTable snapshot(BufferAllocator allocator) {
    snapshot = true;
    memTable.compact();
    return memTable.snapshot(allocator);
  }

  public synchronized MemoryTable snapshot(
      List<Field> fields, RangeSet<Long> ranges, BufferAllocator allocator) {
    snapshot = true;
    memTable.compact();
    return memTable.snapshot(fields, ranges, allocator);
  }

  public synchronized AreaSet<Long, Field> getDeleted() {
    return AreaSet.create(deleted);
  }

  public synchronized void delete(AreaSet<Long, Field> ranges) {
    memTable.delete(ranges);
    if (snapshot) {
      deleted.addAll(ranges);
    }
  }

  public void waitUntilClosed() throws InterruptedException {
    latch.await();
  }

  @Override
  public void close() {
    latch.countDown();
    memTable.close();
    onClose.forEach(NoexceptAutoCloseable::close);
  }
}
