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

import cn.edu.tsinghua.iginx.engine.ContextBuilder;
import cn.edu.tsinghua.iginx.engine.StatementExecutor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.thrift.ExecuteStatementReq;
import cn.edu.tsinghua.iginx.thrift.JobState;
import cn.edu.tsinghua.iginx.transform.api.Runner;
import cn.edu.tsinghua.iginx.transform.exception.TransformException;
import cn.edu.tsinghua.iginx.transform.exec.tools.ExecutionMetaManager;
import cn.edu.tsinghua.iginx.transform.pojo.Job;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@DisallowConcurrentExecution
public class ScheduledJob implements org.quartz.Job {
  private static final Logger LOGGER = LoggerFactory.getLogger(ScheduledJob.class);

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    Job job = (Job) context.getMergedJobDataMap().get("job");
    List<Runner> runnerList = (List<Runner>) context.getMergedJobDataMap().get("runnerList");
    JobRunner jobRunner = (JobRunner) context.getMergedJobDataMap().get("jobRunner");
    if (!jobRunner.beginExecution()) {
      return;
    }
    try {
      execute(job, runnerList, jobRunner);
    } finally {
      jobRunner.endExecution();
    }
  }

  private void execute(Job job, List<Runner> runnerList, JobRunner jobRunner)
      throws JobExecutionException {
    boolean stopOnFailure = job.isStopOnFailure();
    try {
      // [important] the temp table that will be used to store mid-stage result, must be set before
      // run
      for (String table : job.getPyTables()) {
        ExecutionMetaManager.setTempTable(table, job.getJobId());
      }
      for (Runner runner : runnerList) {
        runner.start();
        runner.run();
        runner.close();
      }
    } catch (Exception e) {
      synchronized (job) {
        job.getActive().set(false);
        if (!jobRunner.isClosing()) {
          job.setState(stopOnFailure ? JobState.JOB_FAILING : JobState.JOB_PARTIALLY_FAILING);
        }
        job.setException(e);
      }
      List<Exception> closeExceptions = new ArrayList<>();
      for (Runner runner : runnerList) {
        try {
          runner.close();
        } catch (TransformException closeException) {
          LOGGER.error("Can't close runner for job: {}", job.getJobId(), closeException);
          closeExceptions.add(closeException);
        }
      }
      closeExceptions.forEach(e::addSuppressed);
      // Quartz will automatically unschedule all triggers if stopOnFailure is set.
      throw getJobException(
          "Unexpected error occurred during execution of job: " + job.getJobId(), e, stopOnFailure);
    } finally {
      clearTempTables(job);
    }
    synchronized (job) {
      job.getActive().set(false);
      if (!jobRunner.isClosing()) {
        // Publish IDLE only after this execution's cleanup has completed.
        job.setException(null);
        job.setState(JobState.JOB_IDLE);
      }
    }
  }

  protected void clearTempTables(Job job) {
    if (!job.isTempTableUsed()) {
      return;
    }
    ExecuteStatementReq req =
        new ExecuteStatementReq(
            0,
            "DELETE COLUMNS "
                + ExecutionMetaManager.getTempTableNames().stream()
                    .map(s -> s + ".*")
                    .collect(Collectors.joining(", "))
                + ";");
    RequestContext context = ContextBuilder.getInstance().build(req);
    StatementExecutor.getInstance().execute(context);
    if (context.getResult().getStatus().code != RpcUtils.SUCCESS.code) {
      LOGGER.error(
          "Cannot clear temp tables {} for transform job.",
          ExecutionMetaManager.getTempTableNames());
    }
  }

  private JobExecutionException getJobException(
      String message, Exception e, boolean stopOnFailure) {
    JobExecutionException e2;
    if (e == null) {
      e2 = new JobExecutionException(message);
    } else {
      e2 = new JobExecutionException(message, e);
    }
    if (stopOnFailure) {
      e2.setUnscheduleAllTriggers(true);
    }
    return e2;
  }
}
