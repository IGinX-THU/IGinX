package cn.edu.tsinghua.iginx.engine.physical.task;

import java.util.concurrent.atomic.LongAdder;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class TaskMetrics {

  private final LongAdder affectRows = new LongAdder();
  private final LongAdder span = new LongAdder();

  public void accumulateAffectRows(long number) {
    affectRows.add(number);
  }

  public void accumulateCpuTime(long ms) {
    this.span.add(ms);
  }

  public long affectRows() {
    return affectRows.sum();
  }

  public long cpuTime() {
    return span.sum();
  }
}
