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
package cn.edu.tsinghua.iginx.engine.physical.memory;

import cn.edu.tsinghua.iginx.auth.SessionManager;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.queue.MemoryPhysicalTaskQueue;
import cn.edu.tsinghua.iginx.engine.physical.memory.queue.MemoryPhysicalTaskQueueImpl;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskResult;
import cn.edu.tsinghua.iginx.engine.physical.task.memory.MemoryPhysicalTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryPhysicalTaskDispatcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(MemoryPhysicalTaskDispatcher.class);

  private static final MemoryPhysicalTaskDispatcher INSTANCE = new MemoryPhysicalTaskDispatcher();

  private final MemoryPhysicalTaskQueue taskQueue;

  private final ExecutorService taskDispatcher;

  private final ExecutorService taskExecuteThreadPool;

  private MemoryPhysicalTaskDispatcher() {
    taskQueue = new MemoryPhysicalTaskQueueImpl();
    taskExecuteThreadPool =
        new MemoryTaskThreadPoolExecutor(
            ConfigDescriptor.getInstance().getConfig().getMemoryTaskThreadPoolSize());
    taskDispatcher = Executors.newSingleThreadExecutor();
  }

  public static MemoryPhysicalTaskDispatcher getInstance() {
    return INSTANCE;
  }

  public boolean addMemoryTask(MemoryPhysicalTask<?> task) {
    return taskQueue.addTask(task);
  }

  public void startDispatcher() {
    taskDispatcher.submit(
        () -> {
          try {
            while (true) {
              final MemoryPhysicalTask<?> task = taskQueue.getTask();
              if (isCancelled(task.getContext().getSessionId())) {
                LOGGER.warn(
                    "MemoryPhysicalTask[sessionId={}] is cancelled.",
                    task.getContext().getSessionId());
                continue;
              }
              taskExecuteThreadPool.submit(
                  () -> {
                    MemoryPhysicalTask<?> currentTask = task;
                    while (currentTask != null) {
                      TaskResult<?> result;
                      try {
                        result = currentTask.execute();
                      } catch (Throwable e) {
                        LOGGER.error("execute memory task failure: ", e);
                        result = new TaskResult<>(new PhysicalException(e));
                      }
                      currentTask.setResult(result);
                      if (currentTask.getFollowerTask() != null) { // 链式执行可以被执行的任务
                        MemoryPhysicalTask<?> followerTask =
                            (MemoryPhysicalTask<?>) currentTask.getFollowerTask();
                        if (followerTask.notifyParentReady()) {
                          currentTask = followerTask;
                        } else {
                          currentTask = null;
                        }
                      } else {
                        currentTask = null;
                      }
                    }
                  });
            }
          } catch (Exception e) {
            LOGGER.error(
                "unexpected exception during dispatcher memory task, please contact developer to check: ",
                e);
          }
        });
  }

  private boolean isCancelled(long sessionId) {
    if (sessionId == 0) { // empty ctx
      return false;
    }
    return SessionManager.getInstance().isSessionClosed(sessionId);
  }

  public void stopDispatcher() {
    taskDispatcher.shutdown();
  }
}
