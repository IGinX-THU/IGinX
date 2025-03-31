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
package cn.edu.tsinghua.iginx.resource;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** clean resources for queries if not been accessed for certain time interval. */
public class ResourceCleaner {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResourceCleaner.class);
  private static final String JOB_NAME = "ResourceCleanupJob";
  private static final String JOB_GROUP = "ResourceManagement";
  private static final String TRIGGER_NAME = "QueryCleanupTrigger";

  private Scheduler scheduler;

  public ResourceCleaner() {
    try {
      scheduler = StdSchedulerFactory.getDefaultScheduler();
    } catch (SchedulerException e) {
      LOGGER.error("Can't create scheduler.", e);
    }
  }

  public void startWithInterval(int minutes) {
    try {
      setupCleanupJob(minutes);
      scheduler.start();
    } catch (SchedulerException e) {
      LOGGER.error("Can't create clean up scheduler for queries.", e);
    }
  }

  private void setupCleanupJob(int minutes) throws SchedulerException {
    JobDetail job =
        JobBuilder.newJob(ResourceCleanupJob.class).withIdentity(JOB_NAME, JOB_GROUP).build();

    job.getJobDataMap().put("interval", minutes);

    Trigger trigger =
        TriggerBuilder.newTrigger()
            .withIdentity(TRIGGER_NAME, JOB_GROUP)
            .startNow()
            .withSchedule(
                SimpleScheduleBuilder.simpleSchedule()
                    .withIntervalInMinutes(minutes)
                    .repeatForever())
            .build();
    scheduler.scheduleJob(job, trigger);
  }

  public void shutdown() {
    try {
      scheduler.shutdown();
    } catch (SchedulerException e) {
      LOGGER.error("Can't shutdown scheduled query clean up job.", e);
    }
  }

  public static class ResourceCleanupJob implements Job {

    private final Logger LOGGER = LoggerFactory.getLogger(ResourceCleanupJob.class);

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
      JobDataMap dataMap = context.getJobDetail().getJobDataMap();
      QueryResourceManager manager = QueryResourceManager.getInstance();
      int interval = (int) dataMap.get("interval");

      LOGGER.info("Executing query clean up job.");

      Instant base = Instant.now().minusSeconds(60L * interval);

      List<Exception> closeExceptions = new ArrayList<>();
      for (long id : manager.getQueryIds()) {
        Instant lastAccessTime = manager.getLastAccessTime(id);
        if (!lastAccessTime.isAfter(base)) {
          try {
            manager.releaseQuery(id);
            LOGGER.info("Cleaned resources for query:{}", id);
          } catch (PhysicalException e) {
            LOGGER.error("Can't close resource for query: {}", id, e);
            closeExceptions.add(e);
          }
        }
      }
      if (!closeExceptions.isEmpty()) {
        JobExecutionException err =
            new JobExecutionException("Unexpected error occurred during closing inactive queries");
        closeExceptions.forEach(err::addSuppressed);
        throw err;
      }
    }
  }
}
