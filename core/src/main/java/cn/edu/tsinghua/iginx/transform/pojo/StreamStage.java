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
import cn.edu.tsinghua.iginx.thrift.TaskType;
import cn.edu.tsinghua.iginx.transform.api.Stage;
import cn.edu.tsinghua.iginx.transform.data.ExportWriter;
import java.util.List;

public class StreamStage implements Stage {

  private final DataFlowType dataFlowType;

  private final long sessionId;

  private final Stage beforeStage;

  private final List<Task> taskList;

  private final ExportWriter exportWriter;

  private final boolean startWithIginX;

  public StreamStage(long sessionId, Stage beforeStage, List<Task> taskList, ExportWriter writer) {
    this.dataFlowType = DataFlowType.Stream;
    this.sessionId = sessionId;
    this.beforeStage = beforeStage;
    this.taskList = taskList;
    this.exportWriter = writer;
    this.startWithIginX = taskList.get(0).getTaskType().equals(TaskType.IginX);
  }

  public long getSessionId() {
    return sessionId;
  }

  public Stage getBeforeStage() {
    return beforeStage;
  }

  public List<Task> getTaskList() {
    return taskList;
  }

  public boolean isStartWithIginX() {
    return startWithIginX;
  }

  @Override
  public DataFlowType getStageType() {
    return dataFlowType;
  }

  @Override
  public ExportWriter getExportWriter() {
    return exportWriter;
  }
}
