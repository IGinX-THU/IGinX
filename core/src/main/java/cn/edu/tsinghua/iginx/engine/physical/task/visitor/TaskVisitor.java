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
package cn.edu.tsinghua.iginx.engine.physical.task.visitor;

import cn.edu.tsinghua.iginx.engine.physical.task.BinaryMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.GlobalPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.MultipleMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.UnaryMemoryPhysicalTask;

public interface TaskVisitor {

  /**
   * do sth when you enter a task, this method will be called at the beginning of the 'accept'
   * method.
   */
  default void enter() {}

  /** do sth when you leave a task, this method will be called at the end of 'accept' method. */
  default void leave() {}

  void visit(BinaryMemoryPhysicalTask task);

  void visit(UnaryMemoryPhysicalTask task);

  void visit(MultipleMemoryPhysicalTask task);

  void visit(StoragePhysicalTask task);

  void visit(GlobalPhysicalTask task);
}
