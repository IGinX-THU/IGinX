/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
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
