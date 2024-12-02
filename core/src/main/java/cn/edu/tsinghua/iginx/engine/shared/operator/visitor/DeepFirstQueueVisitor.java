/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
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
