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

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.TRANSFORM_PREFIX;

import cn.edu.tsinghua.iginx.notice.EmailNotifier;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.transform.api.Stage;
import cn.edu.tsinghua.iginx.transform.data.*;
import cn.edu.tsinghua.iginx.utils.EmailFromYAML;
import cn.edu.tsinghua.iginx.utils.JobFromYAML;
import cn.edu.tsinghua.iginx.utils.NotificationFromYAML;
import cn.edu.tsinghua.iginx.utils.TaskFromYAML;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
  private final String name; // will be filename if stored

  private long sessionId;

  private JobState state;
  private final AtomicBoolean active;
  private long startTime;
  private long endTime;
  private Exception exception;

  private boolean needExport;
  private ExportType exportType;
  private String exportFile;
  private ExportWriter writer;

  private final List<Task> taskList;
  private final List<Stage> stageList;
  private EmailNotifier notifier = null;

  private boolean scheduled = false;
  private String scheduleStr = null;
  private final Trigger trigger;
  private boolean stopOnFailure = true;
  private boolean tempTableUsed = false;

  private boolean metaStored = false;
  private final Set<String> pyTables = new HashSet<>();

  public Job(long id, CommitTransformJobReq req) {
    this(id, req, null);
  }

  /**
   * When trigger is not null, this scheduled job's trigger is restored from meta by the name of
   * job's first id. Job yaml file is also stored as first_id.yaml in a specified dir. So we make
   * first id its name. When job automatically restarts after IGinX restarts, the name remains but
   * id will be renewed. SessionId will be -1 if this job is restored, not commited.
   */
  public Job(long id, CommitTransformJobReq req, Trigger trigger) {
    jobId = id;
    sessionId = 0;
    // job should not be bound to a session which could end rapidly.
    // session id = 0 means empty context
    state = JobState.JOB_CREATED;
    active = new AtomicBoolean(false);

    exportType = req.getExportType();
    if (exportType.equals(ExportType.FILE)) {
      needExport = true;
      writer = new FileAppendWriter(req.getFileName());
      exportFile = req.getFileName();
    } else if (exportType.equals(ExportType.IGINX)) {
      needExport = true;
      writer = new IginXWriter(sessionId, TRANSFORM_PREFIX);
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
        if (task.getTaskType().equals(TaskType.SQL) && !stageTasks.isEmpty()) {
          // SQL task will only be stream type. this branch processes SQL task that comes after
          // python tasks: create a new stream stage that starts with this SQL task.
          // Note that SQL tasks will not come after another SQL task because the SQL list can
          // be merged,
          // and such conduct will be prohibited during the yaml reading process.
          // Also, in such SQL task, temp table containing previous python task results could be
          // used. We mark
          // it and clear it after all tasks.
          String previousPythonOutputTableName =
              ((PythonTask) stageTasks.get(stageTasks.size() - 1)).getPyOutputPathPrefix();
          pyTables.add(previousPythonOutputTableName);
          stage =
              new StreamStage(
                  sessionId,
                  stage,
                  new ArrayList<>(stageTasks),
                  new IginXWriter(sessionId, previousPythonOutputTableName));
          stageList.add(stage);
          stageTasks.clear();
          tempTableUsed = true;
        }
        stageTasks.add(task);
      }
    }
    if (!stageTasks.isEmpty()) {
      stage = new StreamStage(sessionId, stage, new ArrayList<>(stageTasks), writer);
      stageList.add(stage);
    }
    if (trigger != null) {
      // recovered from meta
      this.trigger = trigger;
      name = trigger.getKey().getName();
      scheduled = true;
      scheduleStr = req.getSchedule();
    } else {
      name = String.valueOf(jobId);
      // build from string
      if (req.isSetSchedule()) {
        this.trigger = JobScheduleTriggerMaker.getTrigger(req.getSchedule(), jobId);
        scheduled = true;
        scheduleStr = req.getSchedule();
      } else {
        // no schedule information provided. job will be fired instantly
        this.trigger = TriggerBuilder.newTrigger().startNow().build();
      }
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

  public JobFromYAML toYaml() {
    JobFromYAML jobFromYAML = new JobFromYAML();
    jobFromYAML.setExportType(exportType.toString());
    jobFromYAML.setExportFile(exportFile);

    List<TaskFromYAML> taskFromYAMLList = new ArrayList<>();
    for (Task task : taskList) {
      TaskFromYAML taskFromYAML = new TaskFromYAML();
      taskFromYAML.setTaskType(task.getTaskType().toString());
      taskFromYAML.setDataFlowType(task.getDataFlowType().toString());
      taskFromYAML.setTimeout(task.getTimeLimit());
      if (task.isPythonTask()) {
        taskFromYAML.setPyTaskName(((PythonTask) task).getPyTaskName());
        taskFromYAML.setPyOutputPathPrefix(((PythonTask) task).getPyOutputPathPrefix());
      } else {
        taskFromYAML.setSqlList(((SQLTask) task).getSqlList());
      }
      taskFromYAMLList.add(taskFromYAML);
    }

    jobFromYAML.setTaskList(taskFromYAMLList);
    if (scheduleStr != null) {
      jobFromYAML.setSchedule(scheduleStr);
    }

    if (notifier != null) {
      EmailFromYAML emailFromYAML = new EmailFromYAML();
      emailFromYAML.setHostName(notifier.getHostName());
      emailFromYAML.setSmtpPort(notifier.getSmtpPort());
      emailFromYAML.setUserName(notifier.getUsername());
      emailFromYAML.setPassword(notifier.getPassword());
      emailFromYAML.setFrom(notifier.getFrom());
      emailFromYAML.setTo(notifier.getTo());
      NotificationFromYAML notificationFromYAML = new NotificationFromYAML();
      notificationFromYAML.setEmail(emailFromYAML);
      jobFromYAML.setNotification(notificationFromYAML);
    }

    return jobFromYAML;
  }
}
