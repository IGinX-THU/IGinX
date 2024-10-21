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
import cn.edu.tsinghua.iginx.transform.exception.UnknownDataFlowException;
import cn.edu.tsinghua.iginx.transform.pojo.BatchStage;
import cn.edu.tsinghua.iginx.transform.pojo.Job;
import cn.edu.tsinghua.iginx.transform.pojo.StreamStage;
import cn.edu.tsinghua.iginx.transform.pojo.TransformJobFinishListener;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobRunner implements Runner {
  private static final Logger LOGGER = LoggerFactory.getLogger(JobRunner.class);

  private final Job job;

  private final List<Runner> runnerList;

  private Scheduler scheduler;

  public JobRunner(Job job) {
    this.job = job;
    this.runnerList = new ArrayList<>();
  }

  @Override
  public void start() throws UnknownDataFlowException, SchedulerException {
    for (Stage stage : job.getStageList()) {
      DataFlowType dataFlowType = stage.getStageType();
      switch (dataFlowType) {
        case Batch:
          runnerList.add(new BatchStageRunner((BatchStage) stage));
          break;
        case Stream:
          runnerList.add(new StreamStageRunner((StreamStage) stage));
          break;
        default:
          LOGGER.error("Unknown stage type {}", dataFlowType);
          throw new UnknownDataFlowException(dataFlowType);
      }
    }
    scheduler = StdSchedulerFactory.getDefaultScheduler();

    JobDetail jobDetail = JobBuilder.newJob(ScheduledJob.class).build();

    jobDetail.getJobDataMap().put("runnerList", runnerList);
    jobDetail.getJobDataMap().put("job", job);

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
    job.setState(JobState.JOB_IDLE);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(
        // 新起一个线程
        () -> {
          try {
            scheduler.getListenerManager().addTriggerListener(new TransformJobFinishListener());
            LOGGER.info("Starting scheduler...");
            scheduler.start(); // 启动调度器
            LOGGER.info("Scheduler started");
          } catch (SchedulerException e) {
            LOGGER.error("Failed to start scheduler", e);
            job.setState(JobState.JOB_FAILED);
          }
        });
  }

  @Override
  public void close() {
    try {
      scheduler.shutdown();
    } catch (SchedulerException e) {
      LOGGER.error("Fail to close Transform job runner id={}, because", job.getJobId(), e);
    }
  }

  @Override
  public boolean scheduled() {
    return job.isScheduled();
  }
}
