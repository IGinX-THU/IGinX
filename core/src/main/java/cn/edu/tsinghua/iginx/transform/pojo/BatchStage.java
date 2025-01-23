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

import cn.edu.tsinghua.iginx.thrift.DataFlowType;
import cn.edu.tsinghua.iginx.transform.api.Stage;
import cn.edu.tsinghua.iginx.transform.data.ExportWriter;

public class BatchStage implements Stage {

  private final DataFlowType dataFlowType;

  private final Stage beforeStage;

  private final Task task;

  private final ExportWriter exportWriter;

  public BatchStage(Stage beforeStage, Task task, ExportWriter exportWriter) {
    this.dataFlowType = DataFlowType.BATCH;
    this.beforeStage = beforeStage;
    this.task = task;
    this.exportWriter = exportWriter;
  }

  public Stage getBeforeStage() {
    return beforeStage;
  }

  public Task getTask() {
    return task;
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
