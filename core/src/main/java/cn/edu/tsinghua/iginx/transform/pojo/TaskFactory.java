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
package cn.edu.tsinghua.iginx.transform.pojo;

import cn.edu.tsinghua.iginx.thrift.TaskInfo;
import cn.edu.tsinghua.iginx.thrift.TaskType;
import cn.edu.tsinghua.iginx.utils.TaskFromYAML;

public class TaskFactory {

  public static Task getTask(TaskInfo info) {
    TaskType taskType = info.getTaskType();

    if (taskType.equals(TaskType.IginX)) {
      return new IginXTask(info);
    } else if (taskType.equals(TaskType.Python)) {
      return new PythonTask(info);
    } else {
      throw new IllegalArgumentException("Unknown task type: " + taskType.toString());
    }
  }

  public static Task getTask(TaskFromYAML info) {
    String type = info.getTaskType().toLowerCase().trim();
    if (type.equals("iginx")) {
      return new IginXTask(info);
    } else if (type.equals("python")) {
      return new PythonTask(info);
    } else {
      throw new IllegalArgumentException("Unknown task type: " + type);
    }
  }
}
