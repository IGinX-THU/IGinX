package cn.edu.tsinghua.iginx.engine.shared.operator.visitor;

import cn.edu.tsinghua.iginx.engine.shared.operator.BinaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.MultipleOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import java.util.LinkedList;
import java.util.Queue;

public class DeepFirstQueueVisitor implements OperatorVisitor {

  private final Queue<Operator> queue = new LinkedList<>();

  public Queue<Operator> getQueue() {
    return queue;
  }

  @Override
  public void visit(UnaryOperator unaryOperator) {
    queue.offer(unaryOperator);
  }

  @Override
  public void visit(BinaryOperator binaryOperator) {
    queue.offer(binaryOperator);
  }

  @Override
  public void visit(MultipleOperator multipleOperator) {
    queue.offer(multipleOperator);
  }
}
