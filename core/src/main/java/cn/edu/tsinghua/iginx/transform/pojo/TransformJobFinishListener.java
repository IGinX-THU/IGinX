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
package cn.edu.tsinghua.iginx.transform.pojo;

import cn.edu.tsinghua.iginx.thrift.JobState;
import cn.edu.tsinghua.iginx.transform.exception.TransformException;
import cn.edu.tsinghua.iginx.transform.exec.TransformJobManager;
import org.quartz.JobExecutionContext;
import org.quartz.Trigger;
import org.quartz.listeners.TriggerListenerSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransformJobFinishListener extends TriggerListenerSupport {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransformJobFinishListener.class);

  @Override
  public String getName() {
    return "JobFinishListener";
  }

  @Override
  public void triggerFired(Trigger trigger, JobExecutionContext context) {
    // 触发器被触发时的操作
    LOGGER.info(
        "Trigger fired: {}, job:{}",
        trigger.getKey(),
        ((Job) context.getMergedJobDataMap().get("job")).getJobId());
  }

  @Override
  public void triggerMisfired(Trigger trigger) {
    // 触发器错过触发时的操作
    LOGGER.warn("Trigger misfired: {}", trigger.getKey());
  }

  @Override
  public void triggerComplete(
      Trigger trigger,
      JobExecutionContext context,
      Trigger.CompletedExecutionInstruction triggerInstructionCode) {
    if (trigger.getNextFireTime() == null) {
      // 触发器的所有执行结束
      Job job = (Job) context.getMergedJobDataMap().get("job");
      job.setState(JobState.JOB_FINISHED);
      LOGGER.info("Job {} has completed all executions.", job.getJobId());
      try {
        TransformJobManager.getInstance().removeFinishedScheduleJob(job.getJobId());
      } catch (TransformException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
