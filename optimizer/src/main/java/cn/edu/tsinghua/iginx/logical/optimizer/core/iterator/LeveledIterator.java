package cn.edu.tsinghua.iginx.logical.optimizer.core.iterator;

import cn.edu.tsinghua.iginx.engine.shared.operator.BinaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.MultipleOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;

public class LeveledIterator implements TreeIterator {

  private final Queue<Operator> queue = new LinkedList<>();

  public LeveledIterator(Operator root) {
    queue.offer(root);
  }

  @Override
  public boolean hasNext() {
    return !queue.isEmpty();
  }

  @Override
  public Operator next() {
    if (!hasNext()) {
      throw new NoSuchElementException("the iterator has reached the end.");
    }

    Operator op = queue.poll();
    assert op != null;
    OperatorType type = op.getType();

    if (OperatorType.isUnaryOperator(type)) {
      Source source = ((UnaryOperator) op).getSource();
      if (source.getType() == SourceType.Operator) {
        queue.offer(((OperatorSource) source).getOperator());
      }
    } else if (OperatorType.isBinaryOperator(type)) {
      BinaryOperator binaryOp = (BinaryOperator) op;
      Source sourceA = binaryOp.getSourceA();
      Source sourceB = binaryOp.getSourceB();
      queue.offer(((OperatorSource) sourceA).getOperator());
      queue.offer(((OperatorSource) sourceB).getOperator());
    } else {
      MultipleOperator multipleOp = (MultipleOperator) op;
      for (Source source : multipleOp.getSources()) {
        queue.offer(((OperatorSource) source).getOperator());
      }
    }
    return op;
  }
}
