package cn.edu.tsinghua.iginx.engine.physical.task;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;

public class TaskExecuteResult {

  private int affectRows;

  private RowStream rowStream;

  private PhysicalException exception;

  public TaskExecuteResult() {}

  public TaskExecuteResult(RowStream rowStream) {
    this(rowStream, null);
    if (rowStream instanceof Table) {
      Table table = (Table) rowStream;
      affectRows = table.getRowSize();
    }
  }

  public TaskExecuteResult(PhysicalException exception) {
    this(null, exception);
  }

  public TaskExecuteResult(RowStream rowStream, PhysicalException exception) {
    this.rowStream = rowStream;
    this.exception = exception;
  }

  public RowStream getRowStream() {
    RowStream rowStream = this.rowStream;
    this.rowStream = null;
    return rowStream;
  }

  public void setRowStream(RowStream rowStream) {
    this.rowStream = rowStream;
  }

  public PhysicalException getException() {
    return exception;
  }

  public void setException(PhysicalException exception) {
    this.exception = exception;
  }

  public int getAffectRows() {
    return affectRows;
  }
}
