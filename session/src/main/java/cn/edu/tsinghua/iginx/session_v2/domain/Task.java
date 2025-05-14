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
package cn.edu.tsinghua.iginx.session_v2.domain;

import cn.edu.tsinghua.iginx.session_v2.Arguments;
import cn.edu.tsinghua.iginx.thrift.DataFlowType;
import cn.edu.tsinghua.iginx.thrift.TaskType;
import java.util.ArrayList;
import java.util.List;

public class Task {

  private final TaskType taskType;

  private final DataFlowType dataFlowType;

  private final long timeout;

  private final List<String> sqlList;

  private final String pyTaskName;

  private final String pyOutputTable;

  public Task(
      TaskType taskType,
      DataFlowType dataFlowType,
      long timeout,
      List<String> sqlList,
      String pyTaskName,
      String pyOutputTable) {
    this.taskType = taskType;
    this.dataFlowType = dataFlowType;
    this.timeout = timeout;
    this.sqlList = sqlList;
    this.pyTaskName = pyTaskName;
    this.pyOutputTable = pyOutputTable;
  }

  public Task(Task.Builder builder) {
    this(
        builder.taskType,
        builder.dataFlowType,
        builder.timeout,
        builder.sqlList,
        builder.pyTaskName,
        builder.pyOutputTable);
  }

  public static Task.Builder builder() {
    return new Task.Builder();
  }

  public TaskType getTaskType() {
    return taskType;
  }

  public DataFlowType getDataFlowType() {
    return dataFlowType;
  }

  public long getTimeout() {
    return timeout;
  }

  public List<String> getSqlList() {
    return sqlList;
  }

  public String getPyTaskName() {
    return pyTaskName;
  }

  public String getPyOutputTable() {
    return pyOutputTable;
  }

  public static class Builder {

    private TaskType taskType;

    private DataFlowType dataFlowType;

    private long timeout = Long.MAX_VALUE;

    private List<String> sqlList = new ArrayList<>();

    private String pyTaskName;

    private String pyOutputTable;

    public Task.Builder dataFlowType(DataFlowType dataFlowType) {
      this.dataFlowType = dataFlowType;
      return this;
    }

    public Task.Builder timeout(long timeout) {
      this.timeout = timeout;
      return this;
    }

    public Task.Builder sql(String sql) {
      Arguments.checkTaskType(TaskType.IGINX, taskType);
      this.taskType = TaskType.IGINX;
      this.sqlList.add(sql);
      return this;
    }

    public Task.Builder pyTask(String pyTaskName) {
      Arguments.checkTaskType(TaskType.PYTHON, taskType);
      this.taskType = TaskType.PYTHON;
      this.pyTaskName = pyTaskName;
      return this;
    }

    public Task.Builder pyTask(String pyTaskName, String pyOutputTable) {
      Arguments.checkTaskType(TaskType.PYTHON, taskType);
      this.taskType = TaskType.PYTHON;
      this.pyTaskName = pyTaskName;
      this.pyOutputTable = pyOutputTable;
      return this;
    }

    public Task build() {
      Arguments.checkNotNull(taskType, "taskType");
      Arguments.checkNotNull(dataFlowType, "dataFlowType");
      return new Task(this);
    }
  }
}
