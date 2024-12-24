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

public class JobFromYAML {

  private List<TaskFromYAML> taskList;
  private String exportFile;
  private String exportType;
  private String schedule;
  private NotificationFromYAML notification;

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

  public String getSchedule() {
    return schedule;
  }

  public void setSchedule(String schedule) {
    this.schedule = schedule;
  }

  public NotificationFromYAML getNotification() {
    return notification;
  }

  public void setNotification(NotificationFromYAML notification) {
    this.notification = notification;
  }
}
