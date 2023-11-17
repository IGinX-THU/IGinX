package cn.edu.tsinghua.iginx.engine.logical.optimizer.core.iterator;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.visitor.operator.DeepFirstQueueVisitor;
import java.util.Queue;

public class DeepFirstIterator implements TreeIterator {

  private final Queue<Operator> queue;

  public DeepFirstIterator(Operator root) {
    DeepFirstQueueVisitor visitor = new DeepFirstQueueVisitor();
    root.accept(visitor);
    queue = visitor.getQueue();
  }

  @Override
  public boolean hasNext() {
    return !queue.isEmpty();
  }

  @Override
  public Operator next() {
    return queue.poll();
  }
}
