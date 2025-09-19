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

import cn.edu.tsinghua.iginx.thrift.CommitTransformJobReq;
import cn.edu.tsinghua.iginx.thrift.DataFlowType;
import cn.edu.tsinghua.iginx.thrift.Email;
import cn.edu.tsinghua.iginx.thrift.ExportType;
import cn.edu.tsinghua.iginx.thrift.Notification;
import cn.edu.tsinghua.iginx.thrift.TaskInfo;
import cn.edu.tsinghua.iginx.thrift.TaskType;
import java.util.ArrayList;
import java.util.List;

public class JobFromYAML {

  private List<TaskFromYAML> taskList;
  private String exportFile;
  private String exportType;
  private String schedule;
  private NotificationFromYAML notification;
  private boolean stopOnFailure = true;

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

  public boolean isStopOnFailure() {
    return stopOnFailure;
  }

  public void setStopOnFailure(boolean stopOnFailure) {
    this.stopOnFailure = stopOnFailure;
  }

  public CommitTransformJobReq toCommitTransformJobReq(long sessionId) {
    List<TaskInfo> taskList = getTaskInfoList();
    Notification notificationReq = getNotificationReq();

    ExportType type =
        exportType.equalsIgnoreCase("none")
            ? ExportType.LOG
            : ExportType.valueOf(exportType.toUpperCase());

    CommitTransformJobReq req = new CommitTransformJobReq(sessionId, taskList, type);
    req.setFileName(exportFile);
    req.setSchedule(schedule);
    req.setStopOnFailure(stopOnFailure);
    req.setNotification(notificationReq);

    return req;
  }

  private List<TaskInfo> getTaskInfoList() {
    List<TaskInfo> taskList = new ArrayList<>();
    for (int i = 0; i < getTaskList().size(); i++) {
      TaskFromYAML taskFromYAML = getTaskList().get(i);
      TaskType taskType = TaskType.valueOf(taskFromYAML.getTaskType().toUpperCase());
      // SQL task can only be STREAM type, thus can be omitted.
      DataFlowType dataFlowType =
          taskType == TaskType.SQL
              ? DataFlowType.STREAM
              : DataFlowType.valueOf(taskFromYAML.getDataFlowType().toUpperCase());
      TaskInfo task = new TaskInfo(taskType, dataFlowType);
      switch (task.taskType) {
        case SQL:
          task.setSqlList(taskFromYAML.getSqlList());
          break;
        case PYTHON:
          task.setPyTaskName(taskFromYAML.getPyTaskName());
          task.setOutputPrefix(taskFromYAML.getOutputPrefix());
          break;
      }
      task.setTimeout(taskFromYAML.getTimeout());
      taskList.add(task);
    }
    return taskList;
  }

  private Notification getNotificationReq() {
    Email email = null;
    if (notification != null) {
      if (notification.getEmail() != null) {
        EmailFromYAML emailFromYAML = notification.getEmail();
        email =
            new Email(
                emailFromYAML.getHostName(),
                emailFromYAML.getSmtpPort(),
                emailFromYAML.getUserName(),
                emailFromYAML.getPassword(),
                emailFromYAML.getFrom(),
                emailFromYAML.getTo());
      }
    }
    Notification notificationReq = new Notification();
    notificationReq.setEmail(email);
    return notificationReq;
  }
}
