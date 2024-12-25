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

import cn.edu.tsinghua.iginx.notice.EmailNotifier;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.transform.api.Stage;
import cn.edu.tsinghua.iginx.transform.data.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Data;
import org.apache.commons.mail.EmailException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class Job {

  private static final Logger LOGGER = LoggerFactory.getLogger(Job.class);

  private long jobId;

  private long sessionId;

  private JobState state;
  private final AtomicBoolean active;
  private long startTime;
  private long endTime;
  private Exception exception;

  private boolean needExport;
  private ExportType exportType;
  private ExportWriter writer;

  private final List<Task> taskList;
  private final List<Stage> stageList;
  private EmailNotifier notifier = null;

  private boolean scheduled = false;
  private String scheduleStr = null;
  private final Trigger trigger;
  private boolean stopOnFailure = true;

  public Job(long id, CommitTransformJobReq req) {
    jobId = id;
    sessionId = req.getSessionId();
    state = JobState.JOB_CREATED;
    active = new AtomicBoolean(false);

    exportType = req.getExportType();
    if (exportType.equals(ExportType.FILE)) {
      needExport = true;
      writer = new FileAppendWriter(req.getFileName());
    } else if (exportType.equals(ExportType.IGINX)) {
      needExport = true;
      writer = new IginXWriter(req.getSessionId());
    } else {
      needExport = false;
      writer = new LogWriter();
    }

    if (req.isSetStopOnFailure()) {
      stopOnFailure = req.isStopOnFailure();
    }

    taskList = new ArrayList<>();
    stageList = new ArrayList<>();
    Stage stage = null;
    List<Task> stageTasks = new ArrayList<>();
    for (int i = 0; i < req.getTaskListSize(); i++) {
      TaskInfo info = req.getTaskList().get(i);
      Task task = TaskFactory.getTask(info);
      taskList.add(task);

      if (task.getDataFlowType().equals(DataFlowType.BATCH)) {
        if (!stageTasks.isEmpty()) {
          stage =
              new StreamStage(
                  sessionId, stage, new ArrayList<>(stageTasks), new CollectionWriter());
          stageList.add(stage);
          stageTasks.clear();
        }
        if (i == req.getTaskListSize() - 1) {
          stage = new BatchStage(stage, task, writer);
        } else {
          stage = new BatchStage(stage, task, new CollectionWriter());
        }
        stageList.add(stage);
      } else {
        stageTasks.add(task);
      }
    }
    if (!stageTasks.isEmpty()) {
      stage = new StreamStage(sessionId, stage, new ArrayList<>(stageTasks), writer);
      stageList.add(stage);
    }
    if (req.isSetSchedule()) {
      trigger = JobScheduleTriggerMaker.getTrigger(req.getSchedule());
      scheduled = true;
      scheduleStr = req.getSchedule();
    } else {
      // no schedule information provided. job will be fired instantly
      trigger = TriggerBuilder.newTrigger().startNow().build();
    }

    if (req.getNotification() != null) {
      Email email = req.getNotification().getEmail();
      if (email != null) {
        notifier =
            new EmailNotifier(
                email.getHostName(),
                email.getSmtpPort(),
                email.getUsername(),
                email.getPassword(),
                email.getFromAddr(),
                email.getToAddrs());
      }
    }
  }

  public void setState(JobState state) {
    this.state = state;
    switch (state) {
      case JOB_FINISHED:
      case JOB_FAILED:
      case JOB_PARTIALLY_FAILED:
      case JOB_CLOSED:
        try {
          sendEmail();
        } catch (Exception e) {
          LOGGER.error(
              "Fail to send email notification for job {} to {}, because", jobId, notifier, e);
        }
    }
  }

  public void sendEmail() throws EmailException {
    EmailNotifier emailNotifier = getNotifier();
    if (emailNotifier != null) {
      emailNotifier.send(this);
    }
  }

  @Override
  public String toString() {
    return "Job{"
        + "jobId="
        + jobId
        + ", sessionId="
        + sessionId
        + ", state="
        + state
        + ", startTime="
        + startTime
        + ", endTime="
        + endTime
        + ", needExport="
        + needExport
        + ", exportType="
        + exportType
        + ", writer="
        + writer
        + ", taskList="
        + taskList
        + ", stageList="
        + stageList
        + (scheduleStr != null ? ", schedule string=" + scheduleStr : "")
        + '}';
  }
}
