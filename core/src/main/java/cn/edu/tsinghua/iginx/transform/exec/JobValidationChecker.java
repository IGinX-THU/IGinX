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
package cn.edu.tsinghua.iginx.transform.exec;

import cn.edu.tsinghua.iginx.thrift.DataFlowType;
import cn.edu.tsinghua.iginx.thrift.TaskType;
import cn.edu.tsinghua.iginx.transform.api.Checker;
import cn.edu.tsinghua.iginx.transform.pojo.Job;
import cn.edu.tsinghua.iginx.transform.pojo.PythonTask;
import cn.edu.tsinghua.iginx.transform.pojo.SQLTask;
import cn.edu.tsinghua.iginx.transform.pojo.Task;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobValidationChecker implements Checker {
  private static final Logger LOGGER = LoggerFactory.getLogger(JobValidationChecker.class);

  private static final Set<String> pyOutputPathPrefixs = new ConcurrentSkipListSet<>();

  private static JobValidationChecker instance;

  private JobValidationChecker() {}

  public static JobValidationChecker getInstance() {
    if (instance == null) {
      synchronized (JobValidationChecker.class) {
        if (instance == null) {
          instance = new JobValidationChecker();
        }
      }
    }
    return instance;
  }

  @Override
  public boolean check(Job job) {
    pyOutputPathPrefixs.clear();
    List<Task> taskList = job.getTaskList();
    if (taskList == null || taskList.isEmpty()) {
      LOGGER.error("Committed job task list is empty.");
      return false;
    }

    Task firstTask = taskList.get(0);
    if (!firstTask.getTaskType().equals(TaskType.SQL)) {
      LOGGER.error("The first task must be SQL task.");
      return false;
    }

    if (!SQLTaskChecker(firstTask)) {
      return false;
    }

    if (taskList.size() > 1) {
      boolean previousIginX = true;
      for (int i = 1; i < taskList.size(); i++) {
        Task task = taskList.get(i);
        switch (taskList.get(i).getTaskType()) {
          case SQL:
            if (previousIginX) { // previous task is also SQL task
              LOGGER.error(
                  "Please merge multiple SQL tasks into one task by combining the sql statements.");
              return false;
            }
            if (!SQLTaskChecker(task)) {
              return false;
            }
            if (!((PythonTask) taskList.get(i-1)).isSetPyOutputPathPrefix()) {
              LOGGER.error(
                      "The Python task before SQL task must set pyOutputPathPrefix. If you don't feel it necessary, maybe you need to rearrange job stages.");
              return false;
            }
            previousIginX = true;
            break;
          case PYTHON:
            if (!pythonTaskChecker(task)) {
              return false;
            }
            previousIginX = false;
            break;
          default:
            throw new IllegalArgumentException("Unsupported task type: " + task.getTaskType());
        }
      }
    }

    try {
      job.sendEmail();
    } catch (Exception e) {
      LOGGER.error(
          "Fail to send email notification for job {} to {}, because",
          job.getJobId(),
          job.getNotifier(),
          e);
      return false;
    }

    return true;
  }

  private boolean SQLTaskChecker(Task task) {
    if (!task.getTaskType().equals(TaskType.SQL)) {
      LOGGER.error("Expecting SQL task but get {} task.", task.getTaskType());
      return false;
    }
    SQLTask SQLTask = (SQLTask) task;
    if (!SQLTask.getDataFlowType().equals(DataFlowType.STREAM)) {
      LOGGER.error("SQL task must be stream.");
      return false;
    }
    List<String> sqlList = SQLTask.getSqlList();
    if (sqlList == null || sqlList.isEmpty()) {
      LOGGER.error("SQL task should has at least one statement.");
      return false;
    }
    String querySQL = sqlList.get(sqlList.size() - 1);
    if (!querySQL.toLowerCase().trim().startsWith("select")
        && !querySQL.toLowerCase().trim().startsWith("show")) {
      LOGGER.error("SQL task's last statement must be select or showTS statement.");
      return false;
    }
    return true;
  }

  private boolean pythonTaskChecker(Task task) {
    if (!task.getTaskType().equals(TaskType.PYTHON)) {
      LOGGER.error("Expecting Python task but get {} task.", task.getTaskType());
      return false;
    }
    PythonTask pythonTask = (PythonTask) task;
    if (pythonTask.isSetPyOutputPathPrefix()) {
      if (!pythonTask.getPyOutputPathPrefix().matches("[a-zA-Z0-9]+")) {
        LOGGER.error(
            "Python task output table name can only contain numbers or alphabets, got: {}.",
            pythonTask.getPyOutputPathPrefix());
        return false;
      } else if (!pyOutputPathPrefixs.add(pythonTask.getPyOutputPathPrefix())) {
        LOGGER.error(
            "Got duplicated python output table name in different tasks: {}.",
            pythonTask.getPyOutputPathPrefix());
        return false;
      }
    }
    return true;
  }
}
