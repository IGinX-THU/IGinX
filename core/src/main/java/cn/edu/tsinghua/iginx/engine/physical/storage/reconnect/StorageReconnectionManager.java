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
package cn.edu.tsinghua.iginx.engine.physical.storage.reconnect;

import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageReconnectionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageReconnectionManager.class);

  private static volatile StorageReconnectionManager instance;

  private Scheduler scheduler;

  private final Map<StorageEngineMeta, ReconnectState> storageStateMap = new ConcurrentHashMap<>();

  public static StorageReconnectionManager getInstance() {
    if (instance == null) {
      synchronized (StorageReconnectionManager.class) {
        if (instance == null) {
          instance = new StorageReconnectionManager();
        }
      }
    }
    return instance;
  }

  private StorageReconnectionManager() {
    try {
      Properties properties = new Properties();
      properties.setProperty(
          StdSchedulerFactory.PROP_SCHED_INSTANCE_NAME, "StorageReconnectionScheduler");
      properties.setProperty(
          StdSchedulerFactory.PROP_THREAD_POOL_CLASS, "org.quartz.simpl.SimpleThreadPool");
      properties.setProperty(StdSchedulerFactory.PROP_THREAD_POOL_PREFIX + ".threadCount", "1");
      properties.setProperty(
          StdSchedulerFactory.PROP_THREAD_POOL_PREFIX + ".threadNamePrefix", "StorageReconnect-");
      properties.setProperty(
          StdSchedulerFactory.PROP_JOB_STORE_PREFIX + ".class", "org.quartz.simpl.RAMJobStore");
      properties.setProperty(StdSchedulerFactory.PROP_SCHED_INSTANCE_ID, "AUTO");

      StdSchedulerFactory factory = new StdSchedulerFactory(properties);
      scheduler = factory.getScheduler();
      scheduler.start();
    } catch (SchedulerException e) {
      LOGGER.error("Failed to initialize storage reconnection scheduler, caused by ", e);
    }
  }

  public synchronized void addStorageEngine(StorageEngineMeta meta) {
    if (storageStateMap.containsKey(meta)) {
      LOGGER.debug("Storage engine {} is already in reconnection list.", meta);
      return;
    }
    try {
      String jobId = "reconnect_" + meta.getId() + "_" + System.currentTimeMillis();
      JobDataMap jobDataMap = new JobDataMap();
      jobDataMap.put("storageEngineMeta", meta);
      JobDetail jobDetail =
          JobBuilder.newJob(ReconnectJob.class)
              .withIdentity(jobId, "storageReconnectGroup")
              .usingJobData(jobDataMap)
              .build();
      Trigger trigger =
          TriggerBuilder.newTrigger()
              .withIdentity("trigger_" + jobId, "storageReconnectGroup")
              .startNow()
              .build();
      scheduler.scheduleJob(jobDetail, trigger);
      storageStateMap.put(meta, new ReconnectState(jobDetail));
      LOGGER.info("Added storage engine {} to reconnection list.", meta);
    } catch (SchedulerException e) {
      LOGGER.error("Failed to schedule reconnection job for {}", meta, e);
    }
  }

  public synchronized void removeStorageEngine(StorageEngineMeta meta) {
    ReconnectState state = storageStateMap.remove(meta);
    if (state != null && state.getJobDetail() != null) {
      try {
        boolean deleted = scheduler.deleteJob(state.getJobDetail().getKey());
        if (deleted) {
          LOGGER.info("Successfully removed storage engine {} from reconnection list", meta);
        }
      } catch (SchedulerException e) {
        LOGGER.error("Failed to remove reconnection job for {}", meta, e);
      }
    }
  }

  public void shutdown() {
    try {
      if (scheduler != null && !scheduler.isShutdown()) {
        scheduler.shutdown(true);
        LOGGER.info("StorageReconnectionManager has been shutdown");
      }
    } catch (SchedulerException e) {
      LOGGER.error("Error while shutting down storage reconnection scheduler", e);
    } finally {
      storageStateMap.clear();
    }
  }

  public ReconnectState getReconnectState(StorageEngineMeta meta) {
    return storageStateMap.getOrDefault(meta, null);
  }
}
