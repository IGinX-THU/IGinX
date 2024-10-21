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
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;

public interface OperatorVisitor {

  /**
   * do sth when you enter an operator, this method will be called at the beginning of the 'accept'
   * method.
   */
  default void enter() {}

  /**
   * do sth when you leave an operator, this method will be called at the end of 'accept' method.
   */
  default void leave() {}

  /** you can stop the traverse of the operator tree early if you need. */
  default boolean needStop() {
    return false;
  }

  void visit(UnaryOperator unaryOperator);

  void visit(BinaryOperator binaryOperator);

  void visit(MultipleOperator multipleOperator);
}
