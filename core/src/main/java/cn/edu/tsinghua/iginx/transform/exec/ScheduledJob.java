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

import static cn.edu.tsinghua.iginx.transform.utils.Constants.TEMP_TABLE_NAME_FORMAT;

import cn.edu.tsinghua.iginx.thrift.JobState;
import cn.edu.tsinghua.iginx.transform.api.Runner;
import cn.edu.tsinghua.iginx.transform.exception.TransformException;
import cn.edu.tsinghua.iginx.transform.exec.tools.ExecutionMetaManager;
import cn.edu.tsinghua.iginx.transform.pojo.Job;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
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

    job.setState(JobState.JOB_RUNNING);
    boolean stopOnFailure = job.isStopOnFailure();
    if (!job.getActive().compareAndSet(false, true)) {
      throw getJobException(
          "Cannot set active status of job: " + job.getJobId() + ".", null, stopOnFailure);
    }
    try {
      // [important] the temp table that will be used to store mid-stage result, must be set before
      // run
      ExecutionMetaManager.setMeta(
          String.format(
              TEMP_TABLE_NAME_FORMAT, job.getJobId(), RandomStringUtils.randomAlphanumeric(6)));
      for (Runner runner : runnerList) {
        runner.start();
        runner.run();
        runner.close();
      }
      if (job.getActive().compareAndSet(true, false)) {
        // wait for next execution
        // if a trigger has finished all execution, TransformTriggerListener will handle work
        // left.
        job.setState(JobState.JOB_IDLE);
        job.setException(null);
      }
    } catch (Exception e) {
      if (job.getActive().compareAndSet(true, false)) {
        job.setState(stopOnFailure ? JobState.JOB_FAILING : JobState.JOB_PARTIALLY_FAILING);
        job.setException(e);
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
        // Quartz will automatically unschedule
        // all triggers associated with this job
        // so that it does not run again
        throw getJobException(
            "Unexpected error occurred during execution of job: " + job.getJobId(),
            e,
            stopOnFailure);
      }
      throw getJobException(
          "Cannot set active status of job: " + job.getJobId() + ".", e, stopOnFailure);
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
