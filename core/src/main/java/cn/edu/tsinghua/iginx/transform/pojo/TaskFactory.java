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
package cn.edu.tsinghua.iginx.transform.pojo;

import cn.edu.tsinghua.iginx.thrift.TaskInfo;
import cn.edu.tsinghua.iginx.thrift.TaskType;
import cn.edu.tsinghua.iginx.utils.TaskFromYAML;

public class TaskFactory {

  public static Task getTask(TaskInfo info) {
    TaskType taskType = info.getTaskType();

    if (taskType.equals(TaskType.SQL)) {
      return new SQLTask(info);
    } else if (taskType.equals(TaskType.PYTHON)) {
      return new PythonTask(info);
    } else {
      throw new IllegalArgumentException("Unknown task type: " + taskType.toString());
    }
  }

  public static Task getTask(TaskFromYAML info) {
    String type = info.getTaskType().toLowerCase().trim();
    if (type.equals("iginx")) {
      return new SQLTask(info);
    } else if (type.equals("python")) {
      return new PythonTask(info);
    } else {
      throw new IllegalArgumentException("Unknown task type: " + type);
    }
  }
}
