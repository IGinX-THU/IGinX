package cn.edu.tsinghua.iginx.engine.physical.task.memory.parallel;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.executor.util.Batch;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import oshi.annotation.concurrent.ThreadSafe;

@ThreadSafe
public abstract class PrefetchBatchStream implements BatchStream {

  private Batch batch;
  private boolean finished;

  @Override
  public synchronized boolean hasNext() throws PhysicalException {
    if (finished) {
      return false;
    }
    if (batch == null) {
      batch = prefetchBatch();
    }
    if (batch == null) {
      finished = true;
      return false;
    }
    return true;
  }

  @Override
  public synchronized Batch getNext() throws PhysicalException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Batch res = batch;
    batch = null;
    return res;
  }

  @Override
  public synchronized void close() throws PhysicalException {
    finished = true;
    if (batch != null) {
      batch.close();
    }
  }

  @Nullable
  protected abstract Batch prefetchBatch() throws PhysicalException;
}
