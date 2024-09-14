package cn.edu.tsinghua.iginx.engine.physical.task;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.BatchStream;
import java.util.NoSuchElementException;
import java.util.Objects;

public class TaskResult implements AutoCloseable {

  private BatchStream batchStream;

  private PhysicalException exception;

  protected TaskResult(BatchStream batchStream, PhysicalException exception) {
    this.batchStream = batchStream;
    this.exception = exception;
  }

  public TaskResult() {
    this(null, null);
  }

  public TaskResult(BatchStream batchStream) {
    this(Objects.requireNonNull(batchStream), null);
  }

  public TaskResult(PhysicalException exception) {
    this(null, Objects.requireNonNull(exception));
  }

  public boolean isEmpty() {
    return batchStream == null && exception == null;
  }

  public boolean isSuccessful() {
    return exception == null;
  }

  public BatchStream unwrap() throws PhysicalException {
    try {
      if (exception != null) {
        assert batchStream == null;
        throw exception;
      }
      if (batchStream != null) {
        return batchStream;
      }
      throw new NoSuchElementException();
    } finally {
      batchStream = null;
      exception = null;
    }
  }

  @Override
  public void close() throws PhysicalException {
    if (!isEmpty()) {
      unwrap().close();
    }
  }
}
