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

import cn.edu.tsinghua.iginx.thrift.DataFlowType;
import cn.edu.tsinghua.iginx.thrift.TaskInfo;
import cn.edu.tsinghua.iginx.thrift.TaskType;
import cn.edu.tsinghua.iginx.utils.TaskFromYAML;

public class PythonTask extends Task {

  private String pyTaskName = "";

  public PythonTask(TaskInfo info) {
    super(info);
    if (info.isSetPyTaskName()) {
      pyTaskName = info.getPyTaskName().trim();
    } else {
      throw new IllegalArgumentException("Python task must have class name.");
    }
  }

  public PythonTask(TaskFromYAML info) {
    super(info);
    if (info.getPyTaskName() != null) {
      pyTaskName = info.getPyTaskName().trim();
    } else {
      throw new IllegalArgumentException("Python task must have class name.");
    }
  }

  // for test
  public PythonTask(
      TaskType taskType, DataFlowType dataFlowType, long timeLimit, String pyTaskName) {
    super(taskType, dataFlowType, timeLimit);
    this.pyTaskName = pyTaskName.trim();
  }

  public String getPyTaskName() {
    return pyTaskName;
  }
}
