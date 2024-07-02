package cn.edu.tsinghua.iginx.logical.optimizer.core.iterator;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import java.util.Deque;
import java.util.LinkedList;

public class ReverseLeveledIterator implements TreeIterator {

  private final Deque<Operator> stack = new LinkedList<>();

  public ReverseLeveledIterator(Operator root) {
    LeveledIterator it = new LeveledIterator(root);
    while (it.hasNext()) {
      stack.push(it.next());
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
