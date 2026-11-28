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
import cn.edu.tsinghua.iginx.thrift.JobState;
import cn.edu.tsinghua.iginx.transform.api.Runner;
import cn.edu.tsinghua.iginx.transform.api.Stage;
import cn.edu.tsinghua.iginx.transform.exception.TransformException;
import cn.edu.tsinghua.iginx.transform.exception.UnknownDataFlowException;
import cn.edu.tsinghua.iginx.transform.exec.tools.TransformJobListener;
import cn.edu.tsinghua.iginx.transform.exec.tools.TransformTriggerListener;
import cn.edu.tsinghua.iginx.transform.pojo.BatchStage;
import cn.edu.tsinghua.iginx.transform.pojo.Job;
import cn.edu.tsinghua.iginx.transform.pojo.StreamStage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobRunner implements Runner {
  private static final Logger LOGGER = LoggerFactory.getLogger(JobRunner.class);

  private final Job job;

  private final List<Runner> runnerList;

  private Scheduler scheduler;

  private JobKey jobKey;

  // Guarded by job, together with execution/cancellation state transitions.
  private boolean closing;

  private boolean executing;

  public JobRunner(Job job) {
    this.job = job;
    this.runnerList = new ArrayList<>();
  }

  JobRunner(Job job, Scheduler scheduler) {
    this(job);
    this.scheduler = scheduler;
  }

  @Override
  public void start() throws UnknownDataFlowException, SchedulerException {
    for (Stage stage : job.getStageList()) {
      DataFlowType dataFlowType = stage.getStageType();
      switch (dataFlowType) {
        case BATCH:
          runnerList.add(new BatchStageRunner((BatchStage) stage));
          break;
        case STREAM:
          runnerList.add(new StreamStageRunner((StreamStage) stage));
          break;
        default:
          LOGGER.error("Unknown stage type {}", dataFlowType);
          throw new UnknownDataFlowException(dataFlowType);
      }
    }
    if (scheduler == null) {
      scheduler = StdSchedulerFactory.getDefaultScheduler();
    }

    JobDetail jobDetail = JobBuilder.newJob(ScheduledJob.class).build();
    jobKey = jobDetail.getKey();

    jobDetail.getJobDataMap().put("runnerList", runnerList);
    jobDetail.getJobDataMap().put("job", job);
    jobDetail.getJobDataMap().put("jobRunner", this);

    Trigger trigger = job.getTrigger();
    LOGGER.info(
        "Trigger details: StartTime={}, EndTime={}, NextFireTime={}",
        trigger.getStartTime(),
        trigger.getEndTime(),
        trigger.getNextFireTime());

    scheduler.scheduleJob(jobDetail, trigger);
  }

  @Override
  public void run() {
    // idle: waiting for scheduler to fire jobs
    synchronized (job) {
      if (isClosing()) {
        return;
      }
      if (job.getState() == JobState.JOB_CREATED) {
        job.setState(JobState.JOB_IDLE);
      }
    }
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      scheduler.getListenerManager().addJobListener(new TransformJobListener());
      scheduler.getListenerManager().addTriggerListener(new TransformTriggerListener());
      executor.submit(
          () -> {
            try {
              LOGGER.info("Starting scheduler...");
              scheduler.start();
              LOGGER.info("Scheduler started");
            } catch (Exception e) {
              LOGGER.error("Failed to start scheduler", e);
              synchronized (job) {
                if (!isClosing()) {
                  job.setState(JobState.JOB_FAILED);
                }
              }
            }
          });
    } catch (Exception e) {
      LOGGER.error("Failed to setup job listener", e);
      synchronized (job) {
        if (!isClosing()) {
          job.setState(JobState.JOB_FAILED);
        }
      }
    }
  }

  @Override
  public void close() throws TransformException {
    synchronized (job) {
      closing = true;
      job.getActive().set(false);
    }
    try {
      if (scheduler != null && jobKey != null) {
        // The bulk API removes the job and triggers in one store operation. deleteJob's
        // separate trigger lookups can race with Quartz removing a completed trigger.
        scheduler.deleteJobs(Collections.singletonList(jobKey));
      }
    } catch (SchedulerException e) {
      throw new TransformException("Cannot unschedule job with id: " + job.getJobId(), e);
    }
    // Deleting a Quartz job only prevents future triggers. Its current execution, including
    // temporary-table cleanup, must finish before the caller can report JOB_CLOSED.
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    synchronized (job) {
      while (executing) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
          throw new TransformException(
              "Job with id: "
                  + job.getJobId()
                  + " is still closing after 10 seconds. Retry later.");
        }
        try {
          TimeUnit.NANOSECONDS.timedWait(job, remaining);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new TransformException(
              "Interrupted while waiting for job with id: " + job.getJobId() + " to close.", e);
        }
      }
    }
  }

  boolean beginExecution() {
    synchronized (job) {
      // A trigger may already have been acquired when the job is deleted.
      if (isClosing() || executing) {
        return false;
      }
      executing = true;
      job.getActive().set(true);
      job.setState(JobState.JOB_RUNNING);
      return true;
    }
  }

  void endExecution() {
    synchronized (job) {
      executing = false;
      job.notifyAll();
    }
  }

  public boolean isClosing() {
    synchronized (job) {
      return closing
          || job.getState() == JobState.JOB_CLOSING
          || job.getState() == JobState.JOB_CLOSED;
    }
  }

  @Override
  public boolean scheduled() {
    return job.isScheduled();
  }
}
