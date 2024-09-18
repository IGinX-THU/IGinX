package cn.edu.tsinghua.iginx.engine.shared.data.read;

import java.util.Objects;
import javax.annotation.WillCloseWhenClosed;
import org.apache.arrow.vector.table.Table;

public class Batch implements AutoCloseable {

  private final Table table;

  protected Batch(@WillCloseWhenClosed Table table) {
    this.table = Objects.requireNonNull(table);
  }

  @Override
  public void close() {
    table.close();
  }

  public long getRowCount() {
    return table.getRowCount();
  }
}
