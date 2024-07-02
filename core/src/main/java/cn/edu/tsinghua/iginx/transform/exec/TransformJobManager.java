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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.transform.exec;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.notice.EmailNotifier;
import cn.edu.tsinghua.iginx.thrift.CommitTransformJobReq;
import cn.edu.tsinghua.iginx.thrift.JobState;
import cn.edu.tsinghua.iginx.thrift.TaskType;
import cn.edu.tsinghua.iginx.transform.api.Checker;
import cn.edu.tsinghua.iginx.transform.pojo.Job;
import cn.edu.tsinghua.iginx.transform.pojo.PythonTask;
import cn.edu.tsinghua.iginx.transform.pojo.Task;
import cn.edu.tsinghua.iginx.utils.SnowFlakeUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransformJobManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(TransformJobManager.class);

  private final Map<Long, Job> jobMap;

  private final Map<Long, JobRunner> jobRunnerMap;

  private static TransformJobManager instance;

  private final ExecutorService threadPool;

  private final Checker checker = JobValidationChecker.getInstance();

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private TransformJobManager() {
    this.jobMap = new ConcurrentHashMap<>();
    this.jobRunnerMap = new ConcurrentHashMap<>();
    this.threadPool = Executors.newFixedThreadPool(config.getTransformTaskThreadPoolSize());
  }

  public static TransformJobManager getInstance() {
    if (instance == null) {
      synchronized (TransformJobManager.class) {
        if (instance == null) {
          instance = new TransformJobManager();
        }
      }
    }
    return instance;
  }

  public long commit(CommitTransformJobReq jobReq) {
    long jobId = SnowFlakeUtils.getInstance().nextId();
    Job job = new Job(jobId, jobReq);
    return commitJob(job);
  }

  public long commitJob(Job job) {
    if (checker.check(job)) {
      jobMap.put(job.getJobId(), job);
      threadPool.submit(() -> processWithRetry(job, config.getTransformMaxRetryTimes()));
      return job.getJobId();
    } else {
      LOGGER.error("Committed job is illegal.");
      return -1;
    }
  }

  private void processWithRetry(Job job, int retryTimes) {
    // this process will be executed at most retryTimes+1 times.
    for (int processCnt = 0; processCnt <= retryTimes; processCnt++) {
      try {
        process(job);
        processCnt = retryTimes; // don't retry
      } catch (Exception e) {
        LOGGER.error("retry process, executed times: {}", (processCnt + 1));
      }
    }
    EmailNotifier.getInstance().send(job);
  }

  private void process(Job job) throws Exception {
    JobRunner runner = new JobRunner(job);
    job.setStartTime(System.currentTimeMillis());
    try {
      runner.start();
      jobRunnerMap.put(job.getJobId(), runner);
      runner.run();
      jobRunnerMap.remove(job.getJobId()); // since we will retry, we can't do this in finally
    } catch (Exception e) {
      LOGGER.error("Fail to process transform job id={}, because", job.getJobId(), e);
      throw e;
    } finally {
      // TODO: is it legal to retry after runner.close()???
      // TODO:
      // we don't need to close runner for FINISHED or FAILED jobs
      // can we move runner.close() into catch clause?
      runner.close();
    }
    // TODO: should we set end time and log time cost for failed jobs?
    job.setEndTime(System.currentTimeMillis());
    LOGGER.info("Job id={} cost {} ms.", job.getJobId(), job.getEndTime() - job.getStartTime());
  }

  public boolean cancel(long jobId) {
    Job job = jobMap.get(jobId);
    if (job == null) {
      return false;
    }
    JobRunner runner = jobRunnerMap.get(jobId);
    if (runner == null) {
      return false;
    }
    // Since job state is set to FINISHED/FAILING/FAILED before runner removed from
    // jobRunnerMap,
    // if runner == null, we can confirm that job state is not RUNNING or CREATED.
    //
    // Since job state is set before runner removed from jobRunnerMap,
    // even if runner != null, there are still possibilities that
    // job state is FINISHED/FAILING/FAILED.
    // Even worse, we cannot simply avoid this by read before write because of concurrency.
    // Thus, we add an atmoic Boolean field to Job to avoid concurrency.

    // This guard is not reliable under concurrency,
    // but can exclude UNKNOWN state and show our intention
    switch (job.getState()) { // won't be null
      case JOB_RUNNING:
      case JOB_CREATED:
        break; // continue execution
      default:
        return false;
    }
    // atomic guard
    if (!job.getActive().compareAndSet(true, false)) {
      return false;
    }
    // reorder as Normal run: [set-ING,] close, set-ED, remove[, set end time, log time cost].
    job.setState(JobState.JOB_CLOSING);
    runner.close();
    job.setState(JobState.JOB_CLOSED);
    jobRunnerMap.remove(jobId);
    job.setEndTime(System.currentTimeMillis());
    LOGGER.info("Job id={} cost {} ms.", job.getJobId(), job.getEndTime() - job.getStartTime());
    return true;
  }

  public JobState queryJobState(long jobId) {
    if (jobMap.containsKey(jobId)) {
      return jobMap.get(jobId).getState();
    } else {
      return null;
    }
  }

  public List<Long> showEligibleJob(JobState jobState) {
    List<Long> jobIdList = new ArrayList<>();
    for (Job job : jobMap.values()) {
      if (job.getState().equals(jobState)) {
        jobIdList.add(job.getJobId());
      }
    }
    return jobIdList;
  }

  public boolean isRegisterTaskRunning(String taskName) {
    for (Job job : jobMap.values()) {
      JobState jobState = job.getState();
      if (jobState.equals(JobState.JOB_RUNNING) || jobState.equals(JobState.JOB_CREATED)) {
        for (Task task : job.getTaskList()) {
          if (task.getTaskType().equals(TaskType.Python)) {
            PythonTask pythonTask = (PythonTask) task;
            if (pythonTask.getPyTaskName().equals(taskName)) {
              return true;
            }
          }
        }
      }
    }
    return false;
  }
}
