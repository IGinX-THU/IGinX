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

public class JobFromYAML {

  private List<TaskFromYAML> taskList;
  private String exportFile;
  private String exportType;

  public JobFromYAML() {}

  public List<TaskFromYAML> getTaskList() {
    return taskList;
  }

  public void setTaskList(List<TaskFromYAML> taskList) {
    this.taskList = taskList;
  }

  public String getExportFile() {
    return exportFile;
  }

  public void setExportFile(String exportFile) {
    this.exportFile = exportFile;
  }

  public String getExportType() {
    return exportType;
  }

  public void setExportType(String exportType) {
    this.exportType = exportType;
  }
}
