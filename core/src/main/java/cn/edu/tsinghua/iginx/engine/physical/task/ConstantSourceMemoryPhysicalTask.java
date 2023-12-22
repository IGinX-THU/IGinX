package cn.edu.tsinghua.iginx.engine.physical.task;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.UnexpectedOperatorException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.OperatorMemoryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.OperatorMemoryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.physical.task.visitor.TaskVisitor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.ConstantSource;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConstantSourceMemoryPhysicalTask extends MemoryPhysicalTask {

  private static final Logger logger =
      LoggerFactory.getLogger(ConstantSourceMemoryPhysicalTask.class);

  private final RequestContext context;

  public ConstantSourceMemoryPhysicalTask(List<Operator> operators) {
    this(operators, null);
  }

  public ConstantSourceMemoryPhysicalTask(List<Operator> operators, RequestContext context) {
    super(TaskType.ConstantSourceMemory, operators, context);
    this.context = context;
  }

  @Override
  public TaskExecuteResult execute() {
    RowStream stream = null;
    if (getOperators().get(0).getType() == OperatorType.Project) {
      Project project = (Project) getOperators().get(0);
      if (project.getSource().getType() == SourceType.Constant) {
        ConstantSource constantSource = (ConstantSource) project.getSource();
        List<Field> fields = new ArrayList<>();
        Object[] values = new Object[constantSource.getExpressionList().size()];
        for (int i = 0; i < constantSource.getExpressionList().size(); i++) {
          fields.add(new Field(constantSource.getExpressionList().get(i), DataType.FLOAT));
          values[i] = 1.0;
        }
        // 新建RowStream
        Header header = new Header(fields);
        List<Row> rowList = new ArrayList<>();
        rowList.add(new Row(header, values));
        stream = new Table(header, rowList);
      }
    }
    List<Operator> operators = getOperators();
    OperatorMemoryExecutor executor =
        OperatorMemoryExecutorFactory.getInstance().getMemoryExecutor();
    try {
      for (Operator op : operators) {
        if (!OperatorType.isUnaryOperator(op.getType())) {
          throw new UnexpectedOperatorException("unexpected operator " + op + " in unary task");
        }
        stream = executor.executeUnaryOperator((UnaryOperator) op, stream, context);
      }
    } catch (PhysicalException e) {
      logger.error("encounter error when execute operator in memory: ", e);
      return new TaskExecuteResult(e);
    }
    return new TaskExecuteResult(stream);
  }

  @Override
  public boolean notifyParentReady() {
    return parentReadyCount.incrementAndGet() == 1;
  }

  @Override
  public void accept(TaskVisitor visitor) {
    visitor.enter();
    visitor.visit(this);
    visitor.leave();
  }
}
