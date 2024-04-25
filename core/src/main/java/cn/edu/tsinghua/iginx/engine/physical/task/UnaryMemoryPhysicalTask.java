package cn.edu.tsinghua.iginx.engine.physical.task;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.UnexpectedOperatorException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.OperatorMemoryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.OperatorMemoryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.physical.task.visitor.TaskVisitor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Load;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnaryMemoryPhysicalTask extends MemoryPhysicalTask {

  private static final Logger logger = LoggerFactory.getLogger(UnaryMemoryPhysicalTask.class);

  private PhysicalTask parentTask;

  public UnaryMemoryPhysicalTask(
      List<Operator> operators, PhysicalTask parentTask, RequestContext context) {
    super(TaskType.UnaryMemory, operators, context);
    this.parentTask = parentTask;
  }

  public void setParentTask(PhysicalTask parentTask) {
    this.parentTask = parentTask;
  }

  public PhysicalTask getParentTask() {
    return parentTask;
  }

  @Override
  public TaskExecuteResult execute() {
    if (getOperators().size() == 1 && getOperators().get(0).getType().equals(OperatorType.Load)) {
      return executeLoad((Load) getOperators().get(0));
    }
    TaskExecuteResult parentResult = parentTask.getResult();
    if (parentResult == null) {
      return new TaskExecuteResult(
          new PhysicalException("unexpected parent task execute result for " + this + ": null"));
    }
    if (parentResult.getException() != null) {
      return parentResult;
    }
    List<Operator> operators = getOperators();
    RowStream stream = parentResult.getRowStream();
    OperatorMemoryExecutor executor =
        OperatorMemoryExecutorFactory.getInstance().getMemoryExecutor();
    try {
      for (Operator op : operators) {
        if (!OperatorType.isUnaryOperator(op.getType())) {
          throw new UnexpectedOperatorException("unexpected operator " + op + " in unary task");
        }
        stream = executor.executeUnaryOperator((UnaryOperator) op, stream, getContext());
      }
    } catch (PhysicalException e) {
      logger.error("encounter error when execute operator in memory: ", e);
      return new TaskExecuteResult(e);
    }
    return new TaskExecuteResult(stream);
  }

  private TaskExecuteResult executeLoad(Load load) {
    OperatorMemoryExecutor executor =
        OperatorMemoryExecutorFactory.getInstance().getMemoryExecutor();
    try {
      RowStream stream = executor.executeUnaryOperator(load, null, getContext());
      return new TaskExecuteResult(stream);
    } catch (PhysicalException e) {
      logger.error("encounter error when execute load in memory: ", e);
      return new TaskExecuteResult(e);
    }
  }

  @Override
  public boolean notifyParentReady() {
    return parentReadyCount.incrementAndGet() == 1;
  }

  @Override
  public void accept(TaskVisitor visitor) {
    visitor.enter();
    visitor.visit(this);

    PhysicalTask task = getParentTask();
    if (task != null) {
      task.accept(visitor);
    }
    visitor.leave();
  }
}
