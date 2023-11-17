package cn.edu.tsinghua.iginx.engine.shared.visitor.task;

import cn.edu.tsinghua.iginx.engine.physical.task.BinaryMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.GlobalPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.MultipleMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.UnaryMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.stats.OperatorStats;
import java.util.HashMap;
import java.util.Map;

public class StatsVisitor implements TaskVisitor {

  private final Map<OperatorType, OperatorStats> statsMap = new HashMap<>();

  public Map<OperatorType, OperatorStats> getStatsMap() {
    return statsMap;
  }

  @Override
  public void visit(BinaryMemoryPhysicalTask task) {
    collectTaskStats(task);
  }

  @Override
  public void visit(UnaryMemoryPhysicalTask task) {
    collectTaskStats(task);
  }

  @Override
  public void visit(MultipleMemoryPhysicalTask task) {
    collectTaskStats(task);
  }

  @Override
  public void visit(StoragePhysicalTask task) {
    collectTaskStats(task);
  }

  @Override
  public void visit(GlobalPhysicalTask task) {
    collectTaskStats(task);
  }

  private void collectTaskStats(PhysicalTask task) {
    long costTime = task.getSpan();
    long affectRows = task.getAffectedRows();

    // operators.size > 1 if and only if there is PPD
    Operator op = task.getOperators().get(0);
    OperatorType type = op.getType();

    if (statsMap.containsKey(type)) {
      statsMap.get(type).addStats(costTime, affectRows);
    } else {
      statsMap.put(type, new OperatorStats(type, 1, costTime, affectRows));
    }
  }
}
