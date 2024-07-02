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
package cn.edu.tsinghua.iginx.session_v2.domain;

import cn.edu.tsinghua.iginx.session_v2.Arguments;
import cn.edu.tsinghua.iginx.thrift.ExportType;
import java.util.ArrayList;
import java.util.List;

public class Transform {

  private final List<Task> taskList;

  private final ExportType exportType;

  private final String fileName;

  public Transform(List<Task> taskList, ExportType exportType, String fileName) {
    this.taskList = taskList;
    this.exportType = exportType;
    this.fileName = fileName;
  }

  public Transform(Transform.Builder builder) {
    this(builder.taskList, builder.exportType, builder.fileName);
  }

  public static Transform.Builder builder() {
    return new Transform.Builder();
  }

  public List<Task> getTaskList() {
    return taskList;
  }

  public ExportType getExportType() {
    return exportType;
  }

  public String getFileName() {
    return fileName;
  }

  public static class Builder {

    private List<Task> taskList = new ArrayList<>();

    private ExportType exportType;

    private String fileName;

    public Transform.Builder addTask(Task task) {
      taskList.add(task);
      return this;
    }

    public Transform.Builder exportToLog() {
      this.exportType = ExportType.Log;
      return this;
    }

    public Transform.Builder exportToIginX() {
      this.exportType = ExportType.IginX;
      return this;
    }

    public Transform.Builder exportToFile(String fileName) {
      this.exportType = ExportType.File;
      this.fileName = fileName;
      return this;
    }

    public Transform build() {
      Arguments.checkListNonEmpty(taskList, "taskList");
      Arguments.checkNotNull(exportType, "exportType");
      return new Transform(this);
    }
  }
}
