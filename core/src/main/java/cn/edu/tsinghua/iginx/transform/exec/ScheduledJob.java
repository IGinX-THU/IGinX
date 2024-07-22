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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.transform.exec;

import cn.edu.tsinghua.iginx.thrift.JobState;
import cn.edu.tsinghua.iginx.transform.api.Runner;
import cn.edu.tsinghua.iginx.transform.exception.TransformException;
import cn.edu.tsinghua.iginx.transform.pojo.Job;
import java.util.List;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduledJob implements org.quartz.Job {
  private static final Logger LOGGER = LoggerFactory.getLogger(ScheduledJob.class);

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    Job job = (Job) context.getMergedJobDataMap().get("job");
    List<Runner> runnerList = (List<Runner>) context.getMergedJobDataMap().get("runnerList");

    job.setState(JobState.JOB_RUNNING);
    job.getActive().compareAndSet(false, true);
    try {
      for (Runner runner : runnerList) {
        runner.start();
        runner.run();
        runner.close();
      }
      if (job.getActive().compareAndSet(true, false)) {
        // wait for next execution
        job.setState(JobState.JOB_IDLE);
        job.setException(null);
      }
      // if a trigger has finished all execution, TransformJobFinishListener will handle work left.
    } catch (TransformException | SchedulerException e) {
      job.setState(JobState.JOB_FAILING);
      job.setException(e);
      try {
        for (Runner runner : runnerList) {
          runner.close();
        }
      } catch (TransformException closeException) {
        LOGGER.error("can't close job: {}", job.getJobId());
      }
      JobExecutionException e2 = new JobExecutionException(e);
      // Quartz will automatically unschedule
      // all triggers associated with this job
      // so that it does not run again
      e2.setUnscheduleAllTriggers(true);
      throw e2;
    }
  }
}
