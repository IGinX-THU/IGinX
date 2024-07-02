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
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskType;
import cn.edu.tsinghua.iginx.engine.physical.task.UnaryMemoryPhysicalTask;
import java.util.ArrayList;
import java.util.List;

public class TaskInfoVisitor implements TaskVisitor {

  private final List<Object[]> cache = new ArrayList<>();

  private int maxLen = 0;

  private int depth = -1;

  public List<Object[]> getCache() {
    return cache;
  }

  public int getMaxLen() {
    return maxLen;
  }

  @Override
  public void enter() {
    depth++;
  }

  @Override
  public void leave() {
    depth--;
  }

  @Override
  public void visit(BinaryMemoryPhysicalTask task) {
    collectTaskInfo(task);
  }

  @Override
  public void visit(UnaryMemoryPhysicalTask task) {
    collectTaskInfo(task);
  }

  @Override
  public void visit(MultipleMemoryPhysicalTask task) {
    collectTaskInfo(task);
  }

  @Override
  public void visit(StoragePhysicalTask task) {
    collectTaskInfo(task);
  }

  @Override
  public void visit(GlobalPhysicalTask task) {
    collectTaskInfo(task);
  }

  private void collectTaskInfo(PhysicalTask task) {
    TaskType type = task.getType();
    StringBuilder builder = new StringBuilder();
    if (depth != 0) {
      for (int i = 0; i < depth; i++) {
        builder.append("  ");
      }
      builder.append("+--");
    }
    builder.append(type);

    maxLen = Math.max(maxLen, builder.length());

    Object[] values = new Object[5];
    values[0] = builder.toString();
    values[1] = (task.getSpan() + "ms").getBytes();
    values[2] = task.getType().toString().getBytes();
    values[3] = task.getInfo().getBytes();
    values[4] = task.getAffectedRows();
    cache.add(values);
  }
}
