package cn.edu.tsinghua.iginx.engine.physical.task.visitor;

import cn.edu.tsinghua.iginx.engine.physical.task.BinaryMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.GlobalPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.MultipleMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.UnaryMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import java.util.Map;

public class TaskStatsVisitor implements TaskVisitor {

  private final Map<OperatorType, Long> operatorCounterMap;

  private final Map<OperatorType, Long> operatorSpanMap;

  private final Map<OperatorType, Long> operatorRowsCountMap;

  public TaskStatsVisitor(
      Map<OperatorType, Long> operatorCounterMap,
      Map<OperatorType, Long> operatorSpanMap,
      Map<OperatorType, Long> operatorRowsCountMap) {
    this.operatorCounterMap = operatorCounterMap;
    this.operatorSpanMap = operatorSpanMap;
    this.operatorRowsCountMap = operatorRowsCountMap;
  }

  @Override
  public void visit(BinaryMemoryPhysicalTask task) {
    collectTaskInfo(task);
  }

  @Override
  public void visit(UnaryMemoryPhysicalTask task) {
    collectTaskInfo(task);
  }

  @Override
  public void visit(MultipleMemoryPhysicalTask task) {
    collectTaskInfo(task);
  }

  @Override
  public void visit(StoragePhysicalTask task) {
    collectTaskInfo(task);
  }

  @Override
  public void visit(GlobalPhysicalTask task) {
    collectTaskInfo(task);
  }

  private void collectTaskInfo(PhysicalTask task) {
    OperatorType type = task.getOperators().get(0).getType();
    operatorCounterMap.merge(type, 1L, Long::sum);
    operatorSpanMap.merge(type, task.getSpan(), Long::sum);
    operatorRowsCountMap.merge(type, (long) task.getAffectedRows(), Long::sum);
  }
}
