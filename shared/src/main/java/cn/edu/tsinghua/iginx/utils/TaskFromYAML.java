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
package cn.edu.tsinghua.iginx.utils;

import java.util.List;

public class TaskFromYAML {

  private String taskType;
  private String dataFlowType;
  private long timeout;
  private String pyTaskName;
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

  public List<String> getSqlList() {
    return sqlList;
  }

  public void setSqlList(List<String> sqlList) {
    this.sqlList = sqlList;
  }
}
