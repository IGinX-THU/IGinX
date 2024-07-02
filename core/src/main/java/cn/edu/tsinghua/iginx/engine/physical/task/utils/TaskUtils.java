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
package cn.edu.tsinghua.iginx.engine.physical.task.utils;

import cn.edu.tsinghua.iginx.engine.physical.task.BinaryMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.MultipleMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.UnaryMemoryPhysicalTask;
import java.util.List;

public class TaskUtils {

  public static void getBottomTasks(List<PhysicalTask> tasks, PhysicalTask root) {
    if (root == null) {
      return;
    }
    switch (root.getType()) {
      case Storage:
      case Global:
        tasks.add(root);
        break;
      case BinaryMemory:
        BinaryMemoryPhysicalTask binaryMemoryPhysicalTask = (BinaryMemoryPhysicalTask) root;
        getBottomTasks(tasks, binaryMemoryPhysicalTask.getParentTaskA());
        getBottomTasks(tasks, binaryMemoryPhysicalTask.getParentTaskB());
        break;
      case UnaryMemory:
        UnaryMemoryPhysicalTask unaryMemoryPhysicalTask = (UnaryMemoryPhysicalTask) root;
        getBottomTasks(tasks, unaryMemoryPhysicalTask.getParentTask());
        break;
      case MultipleMemory:
        MultipleMemoryPhysicalTask multipleMemoryPhysicalTask = (MultipleMemoryPhysicalTask) root;
        for (PhysicalTask parentTask : multipleMemoryPhysicalTask.getParentTasks()) {
          getBottomTasks(tasks, parentTask);
        }
        break;
      default:
        throw new RuntimeException("unknown task type: " + root.getType());
    }
  }
}
