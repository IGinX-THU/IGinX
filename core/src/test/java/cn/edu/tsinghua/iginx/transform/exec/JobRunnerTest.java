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

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.thrift.CommitTransformJobReq;
import cn.edu.tsinghua.iginx.thrift.ExportType;
import cn.edu.tsinghua.iginx.thrift.JobState;
import cn.edu.tsinghua.iginx.transform.exception.TransformException;
import cn.edu.tsinghua.iginx.transform.exec.tools.TransformTriggerListener;
import cn.edu.tsinghua.iginx.transform.pojo.Job;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.listeners.JobListenerSupport;

public class JobRunnerTest {
  private Scheduler scheduler;
  private Job job;
  private JobRunner runner;
  private TransformJobManager manager;
  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private final CountDownLatch cleanupEntered = new CountDownLatch(1);
  private final CountDownLatch cleanupRelease = new CountDownLatch(1);
  private final CountDownLatch executionComplete = new CountDownLatch(1);
  private final CountDownLatch acquiredRelease = new CountDownLatch(1);
  private final AtomicInteger cleanupCount = new AtomicInteger();
  private final AtomicReference<JobExecutionContext> context = new AtomicReference<>();

  @Before
  public void setUp() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(
        "org.quartz.scheduler.instanceName", "JobRunnerTest-" + UUID.randomUUID());
    properties.setProperty("org.quartz.threadPool.threadCount", "2");
    properties.setProperty("org.quartz.jobStore.class", "org.quartz.simpl.RAMJobStore");
    scheduler = new StdSchedulerFactory(properties).getScheduler();
    scheduler.setJobFactory(
        (bundle, ignored) ->
            new ScheduledJob() {
              @Override
              protected void clearTempTables(Job job) {
                cleanupCount.incrementAndGet();
                cleanupEntered.countDown();
                await(cleanupRelease, 15);
              }
            });
    scheduler
        .getListenerManager()
        .addJobListener(
            new JobListenerSupport() {
              @Override
              public String getName() {
                return "execution-complete";
              }

              @Override
              public void jobWasExecuted(
                  JobExecutionContext executionContext, JobExecutionException error) {
                context.set(executionContext);
                executionComplete.countDown();
              }
            });
    CommitTransformJobReq request = new CommitTransformJobReq();
    request.setExportType(ExportType.LOG);
    request.setTaskList(Collections.emptyList());
    job = new Job(1, request);
    runner = new JobRunner(job, scheduler);
    runner.start();
    IMetaManager metadata =
        (IMetaManager)
            Proxy.newProxyInstance(
                IMetaManager.class.getClassLoader(),
                new Class<?>[] {IMetaManager.class},
                (proxy, method, args) -> {
                  if (method.getName().equals("getTransformJobs")) {
                    return Collections.emptyList();
                  }
                  throw new AssertionError("Unexpected metadata call: " + method.getName());
                });
    manager = new TransformJobManager(metadata);
    // Register the real scheduler/runner without submitting SQL or starting external services.
    register("jobMap", job);
    register("jobRunnerMap", runner);
  }

  @SuppressWarnings("unchecked")
  private void register(String fieldName, Object value) throws Exception {
    Field field = TransformJobManager.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    ((Map<Long, Object>) field.get(manager)).put(job.getJobId(), value);
  }

  @After
  public void tearDown() throws Exception {
    cleanupRelease.countDown();
    acquiredRelease.countDown();
    executor.shutdownNow();
    if (scheduler != null) {
      scheduler.shutdown(true);
    }
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
  }

  @Test(timeout = 15000)
  public void closeWaitsForTemporaryTableCleanup() throws Exception {
    scheduler.start();
    await(cleanupEntered);
    assertEquals(JobState.JOB_RUNNING, job.getState());
    Future<?> close =
        executor.submit(
            () -> {
              assertTrue(manager.cancel(job.getJobId()));
              return null;
            });
    assertCloseIsWaiting(close);
    cleanupRelease.countDown();
    close.get(5, TimeUnit.SECONDS);
    await(executionComplete);
    assertEquals(1, cleanupCount.get());
    assertTrue(runner.isClosing());
    assertEquals(JobState.JOB_CLOSED, manager.queryJobState(job.getJobId()));
  }

  @Test(timeout = 15000)
  public void idleIsPublishedAfterCleanup() throws Exception {
    scheduler.start();
    await(cleanupEntered);
    assertEquals(JobState.JOB_RUNNING, job.getState());
    cleanupRelease.countDown();
    await(executionComplete);
    assertEquals(JobState.JOB_IDLE, job.getState());
    assertFalse(job.getActive().get());
  }

  @Test(timeout = 15000)
  public void acquiredTriggerCannotStartAfterClose() throws Exception {
    CountDownLatch acquired = new CountDownLatch(1);
    scheduler
        .getListenerManager()
        .addJobListener(
            new JobListenerSupport() {
              @Override
              public String getName() {
                return "pause-acquired-trigger";
              }

              @Override
              public void jobToBeExecuted(JobExecutionContext executionContext) {
                acquired.countDown();
                await(acquiredRelease);
              }
            });
    scheduler.start();
    await(acquired);
    assertTrue(manager.cancel(job.getJobId()));
    acquiredRelease.countDown();
    await(executionComplete);
    assertEquals(0, cleanupCount.get());
    assertFalse(job.getActive().get());
    assertEquals(JobState.JOB_CLOSED, job.getState());
  }

  @Test(timeout = 15000)
  public void interruptedCloseCanBeRetriedWithoutStartingAnotherExecution() throws Exception {
    scheduler.start();
    await(cleanupEntered);
    Future<Boolean> interrupted =
        executor.submit(
            () -> {
              Thread.currentThread().interrupt();
              try {
                manager.cancel(job.getJobId());
                fail("Closing an executing job must wait and propagate interruption");
                return false;
              } catch (TransformException e) {
                assertTrue(e.getCause() instanceof InterruptedException);
                return Thread.currentThread().isInterrupted();
              } finally {
                Thread.interrupted();
              }
            });
    assertTrue(interrupted.get(5, TimeUnit.SECONDS));
    assertEquals(JobState.JOB_CLOSING, manager.queryJobState(job.getJobId()));
    Future<?> retry =
        executor.submit(
            () -> {
              assertTrue(manager.cancel(job.getJobId()));
              return null;
            });
    assertCloseIsWaiting(retry);
    cleanupRelease.countDown();
    retry.get(5, TimeUnit.SECONDS);
    await(executionComplete);
    new ScheduledJob().execute(context.get());
    assertEquals(1, cleanupCount.get());
    assertEquals(JobState.JOB_CLOSED, manager.queryJobState(job.getJobId()));
  }

  @Test(timeout = 20000)
  public void timedOutCloseCanBeRetriedAfterCleanup() throws Exception {
    scheduler.start();
    await(cleanupEntered);
    try {
      manager.cancel(job.getJobId());
      fail("Close must not succeed while cleanup is still running");
    } catch (TransformException expected) {
      assertTrue(expected.getMessage().contains("still closing"));
    }
    assertEquals(JobState.JOB_CLOSING, job.getState());
    cleanupRelease.countDown();
    assertTrue(manager.cancel(job.getJobId()));
    await(executionComplete);
    assertEquals(1, cleanupCount.get());
    assertEquals(JobState.JOB_CLOSED, manager.queryJobState(job.getJobId()));
  }

  @Test(timeout = 15000)
  public void schedulerFailureIsReportedToCaller() throws Exception {
    scheduler.shutdown(true);
    try {
      manager.cancel(job.getJobId());
      fail("Unscheduling failure must be reported to the caller");
    } catch (TransformException expected) {
      assertTrue(expected.getCause() instanceof SchedulerException);
    }
    assertEquals(JobState.JOB_CLOSING, manager.queryJobState(job.getJobId()));
  }

  @Test(timeout = 15000)
  public void lateCompletionDoesNotOverwriteCancellationState() throws Exception {
    scheduler.start();
    await(cleanupEntered);
    job.setState(JobState.JOB_CLOSING);
    cleanupRelease.countDown();
    await(executionComplete);
    runner.close();
    TransformTriggerListener listener = new TransformTriggerListener();
    JobExecutionContext completed = context.get();
    listener.triggerComplete(
        completed.getTrigger(), completed, Trigger.CompletedExecutionInstruction.NOOP);
    assertEquals(JobState.JOB_CLOSING, job.getState());
    job.setState(JobState.JOB_CLOSED);
    job.setException(new TransformException("failure during cancellation"));
    listener.triggerComplete(
        completed.getTrigger(), completed, Trigger.CompletedExecutionInstruction.NOOP);
    assertEquals(JobState.JOB_CLOSED, job.getState());
  }

  @Test(timeout = 15000)
  public void completionCallbackCanCloseItsOwnRunner() throws Exception {
    CountDownLatch callbackComplete = new CountDownLatch(1);
    AtomicReference<Exception> error = new AtomicReference<>();
    scheduler
        .getListenerManager()
        .addJobListener(
            new JobListenerSupport() {
              @Override
              public String getName() {
                return "close-after-execution";
              }

              @Override
              public void jobWasExecuted(
                  JobExecutionContext executionContext, JobExecutionException exception) {
                try {
                  runner.close();
                } catch (TransformException e) {
                  error.set(e);
                } finally {
                  callbackComplete.countDown();
                }
              }
            });
    cleanupRelease.countDown();
    scheduler.start();
    await(callbackComplete);
    assertNull(error.get());
  }

  private static void assertCloseIsWaiting(Future<?> close) throws Exception {
    try {
      close.get(200, TimeUnit.MILLISECONDS);
      fail("Close returned before temporary-table cleanup finished");
    } catch (TimeoutException expected) {
      // Cleanup is deliberately held by a latch, so successful closure is impossible here.
    }
  }

  private static void await(CountDownLatch latch) {
    await(latch, 5);
  }

  private static void await(CountDownLatch latch, int seconds) {
    try {
      assertTrue("Timed out waiting for test execution", latch.await(seconds, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }
}
