package cn.edu.tsinghua.iginx.engine.shared.stats;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;

public class OperatorStats {

  private final OperatorType type;

  private int count = 0;

  private long costTime = 0;

  private long affectRows = 0;

  public OperatorStats(OperatorType type) {
    this.type = type;
  }

  public OperatorStats(OperatorType type, int count, long costTime, long affectRows) {
    this.type = type;
    this.count = count;
    this.costTime = costTime;
    this.affectRows = affectRows;
  }

  public void addStats(long costTime, long affectRows) {
    this.count++;
    this.costTime += costTime;
    this.affectRows += affectRows;
  }

  public void addStats(OperatorStats other) {
    this.count += other.count;
    this.costTime += other.costTime;
    this.affectRows += other.affectRows;
  }

  public OperatorType getType() {
    return type;
  }

  public int getCount() {
    return count;
  }

  public long getCostTime() {
    return costTime;
  }

  public long getAffectRows() {
    return affectRows;
  }
}
