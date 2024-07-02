package cn.edu.tsinghua.iginx.engine.physical.memory;

import cn.edu.tsinghua.iginx.auth.SessionManager;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.queue.MemoryPhysicalTaskQueue;
import cn.edu.tsinghua.iginx.engine.physical.memory.queue.MemoryPhysicalTaskQueueImpl;
import cn.edu.tsinghua.iginx.engine.physical.task.MemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
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
        Executors.newFixedThreadPool(
            ConfigDescriptor.getInstance().getConfig().getMemoryTaskThreadPoolSize());
    taskDispatcher = Executors.newSingleThreadExecutor();
  }

  public static MemoryPhysicalTaskDispatcher getInstance() {
    return INSTANCE;
  }

  public boolean addMemoryTask(MemoryPhysicalTask task) {
    return taskQueue.addTask(task);
  }

  public void startDispatcher() {
    taskDispatcher.submit(
        () -> {
          try {
            while (true) {
              final MemoryPhysicalTask task = taskQueue.getTask();
              if (isCancelled(task.getSessionId())) {
                LOGGER.warn("MemoryPhysicalTask[sessionId={}] is cancelled.", task.getSessionId());
                continue;
              }
              taskExecuteThreadPool.submit(
                  () -> {
                    MemoryPhysicalTask currentTask = task;
                    while (currentTask != null) {
                      TaskExecuteResult result;
                      long startTime = System.currentTimeMillis();
                      try {
                        result = currentTask.execute();
                      } catch (Exception e) {
                        LOGGER.error("execute memory task failure: ", e);
                        result = new TaskExecuteResult(new PhysicalException(e));
                      }
                      long span = System.currentTimeMillis() - startTime;
                      currentTask.setSpan(span);
                      currentTask.setResult(result);
                      if (currentTask.getFollowerTask() != null) { // 链式执行可以被执行的任务
                        MemoryPhysicalTask followerTask =
                            (MemoryPhysicalTask) currentTask.getFollowerTask();
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
