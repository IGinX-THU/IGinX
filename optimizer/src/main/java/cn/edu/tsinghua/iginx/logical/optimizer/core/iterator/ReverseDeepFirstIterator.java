package cn.edu.tsinghua.iginx.logical.optimizer.core.iterator;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.visitor.DeepFirstQueueVisitor;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Queue;

public class ReverseDeepFirstIterator implements TreeIterator {
  private final Deque<Operator> stack = new LinkedList<>();

  public ReverseDeepFirstIterator(Operator root) {
    DeepFirstQueueVisitor visitor = new DeepFirstQueueVisitor();
    root.accept(visitor);
    Queue<Operator> forward = visitor.getQueue();
    while (!forward.isEmpty()) {
      stack.push(forward.poll());
    }
  }

  @Override
  public boolean hasNext() {
    return !stack.isEmpty();
  }

  @Override
  public Operator next() {
    return stack.poll();
  }
}
