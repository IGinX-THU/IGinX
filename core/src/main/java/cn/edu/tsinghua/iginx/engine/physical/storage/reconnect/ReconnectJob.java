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

import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngineImpl;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.StorageManager;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import java.util.Collections;
import java.util.Date;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@DisallowConcurrentExecution
@PersistJobDataAfterExecution
public class ReconnectJob implements Job {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReconnectJob.class);

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    IMetaManager metaManager = DefaultMetaManager.getInstance();
    StorageReconnectionManager storageReconnectionManager =
        StorageReconnectionManager.getInstance();

    JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
    StorageEngineMeta meta = (StorageEngineMeta) jobDataMap.get("storageEngineMeta");

    ReconnectState state = storageReconnectionManager.getReconnectState(meta);
    if (state == null) {
      LOGGER.warn("No reconnect state found for {}, removing from reconnection list.", meta);
      storageReconnectionManager.removeStorageEngine(meta);
      return;
    }

    // 通过其他方式连接上了该storage，停止尝试重启
    if (metaManager.getConnectStorageEngines().contains(meta)) {
      LOGGER.info("Storage {} has been reconnected, removing from reconnection list.", meta);
      storageReconnectionManager.removeStorageEngine(meta);
      return;
    }

    IStorage storage = StorageManager.initStorageInstance(meta);
    if (storage != null
        && PhysicalEngineImpl.getInstance().getStorageManager().addStorage(meta, storage)) {
      metaManager.addStorageConnection(Collections.singletonList(meta));
      LOGGER.info("Successfully reconnect to storage {}.", meta);
      storageReconnectionManager.removeStorageEngine(meta);
      return;
    }

    // 重新调度任务
    try {
      String triggerId = "trigger_reconnect_" + meta.getId() + "_" + System.currentTimeMillis();
      Trigger newTrigger =
          TriggerBuilder.newTrigger()
              .forJob(context.getJobDetail().getKey())
              .withIdentity(triggerId, "storageReconnectGroup")
              .startAt(new Date(System.currentTimeMillis() + state.calculateNextInterval() * 1000))
              .build();
      context.getScheduler().rescheduleJob(context.getTrigger().getKey(), newTrigger);
      LOGGER.info("Scheduled next reconnection attempt for {}", meta);
    } catch (SchedulerException e) {
      LOGGER.error("Error during reconnection attempt for {}", meta, e);
    }
  }
}
