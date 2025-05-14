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
package cn.edu.tsinghua.iginx.utils;

import java.util.List;

public class TaskFromYAML {

  private String taskType;
  private String dataFlowType;
  private long timeout;
  private String pyTaskName;
  private String pyOutputTable;
  private List<String> sqlList;

  public TaskFromYAML() {}

  public String getTaskType() {
    return taskType;
  }

  public void setTaskType(String taskType) {
    this.taskType = taskType;
  }

  public String getDataFlowType() {
    return dataFlowType;
  }

  public void setDataFlowType(String dataFlowType) {
    this.dataFlowType = dataFlowType;
  }

  public long getTimeout() {
    return timeout;
  }

  public void setTimeout(long timeout) {
    this.timeout = timeout;
  }

  public String getPyTaskName() {
    return pyTaskName;
  }

  public void setPyTaskName(String pyTaskName) {
    this.pyTaskName = pyTaskName;
  }

  public String getPyOutputTable() {
    return pyOutputTable;
  }

  public void setPyOutputTable(String pyOutputTable) {
    this.pyOutputTable = pyOutputTable;
  }

  public boolean isSetPyOutputTable() {
    return pyOutputTable != null;
  }

  public List<String> getSqlList() {
    return sqlList;
  }

  public void setSqlList(List<String> sqlList) {
    this.sqlList = sqlList;
  }
}
