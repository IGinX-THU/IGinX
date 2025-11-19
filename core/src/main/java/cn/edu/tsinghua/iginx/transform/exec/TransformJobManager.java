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

import static cn.edu.tsinghua.iginx.transform.pojo.TriggerDescriptor.toTriggerDescriptor;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.TransformJobMeta;
import cn.edu.tsinghua.iginx.thrift.CommitTransformJobReq;
import cn.edu.tsinghua.iginx.thrift.JobState;
import cn.edu.tsinghua.iginx.thrift.TaskType;
import cn.edu.tsinghua.iginx.transform.api.Checker;
import cn.edu.tsinghua.iginx.transform.exception.TransformException;
import cn.edu.tsinghua.iginx.transform.pojo.Job;
import cn.edu.tsinghua.iginx.transform.pojo.PythonTask;
import cn.edu.tsinghua.iginx.transform.pojo.Task;
import cn.edu.tsinghua.iginx.transform.pojo.TriggerDescriptor;
import cn.edu.tsinghua.iginx.utils.JobFromYAML;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.SnowFlakeUtils;
import cn.edu.tsinghua.iginx.utils.YAMLReader;
import cn.edu.tsinghua.iginx.utils.YAMLWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransformJobManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(TransformJobManager.class);

  private static final IMetaManager metaManager = DefaultMetaManager.getInstance();

  private final Map<Long, Job> jobMap;

  private final Map<Long, JobRunner> jobRunnerMap;

  private static TransformJobManager instance;

  private final ExecutorService threadPool;

  private final Checker checker = JobValidationChecker.getInstance();

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final String JobYamlDir = config.getDefaultScheduledTransformJobDir();

  private TransformJobManager() {
    this.jobMap = new ConcurrentHashMap<>();
    this.jobRunnerMap = new ConcurrentHashMap<>();
    this.threadPool = Executors.newFixedThreadPool(config.getTransformTaskThreadPoolSize());
    initScheduledJobs();
    File file = new File(JobYamlDir);
    if (!file.exists()) {
      file.mkdirs();
    }
  }

  private void initScheduledJobs() {
    List<TransformJobMeta> descriptors = metaManager.getTransformJobs();
    String path;
    for (TransformJobMeta jobMeta : descriptors) {
      if (!jobMeta.getIp().equals(config.getIp()) || jobMeta.getPort() != config.getPort()) {
        // not on this iginx node
        continue;
      }
      Trigger trigger = TriggerDescriptor.fromTriggerDescriptor(jobMeta.getTrigger());
      path = String.join(File.separator, JobYamlDir, jobMeta.getName() + ".yaml");
      if (trigger == null) {
        LOGGER.error(
            "Illegal trigger jobMeta or all executions have been missed: {}. Trigger will be removed.",
            jobMeta);
        metaManager.dropTransformJob(jobMeta.getName());
        try {
          Files.deleteIfExists(Paths.get(path));
        } catch (IOException e) {
          LOGGER.error("Cannot delete yaml file {}", path, e);
        }
        continue;
      }
      if (!(jobMeta.getTrigger()).equals(Objects.requireNonNull(toTriggerDescriptor(trigger)))) {
        // in type.EVERY trigger, start time might be updated
        jobMeta.setTrigger(TriggerDescriptor.toTriggerDescriptor(trigger));
        metaManager.updateTransformJob(jobMeta);
      }
      try {
        LOGGER.debug("Reading existing job from yaml:{}.", new File(path).getCanonicalPath());
        YAMLReader reader = new YAMLReader(path);
        JobFromYAML jobFromYAML = reader.getJobFromYAML();

        long id = SnowFlakeUtils.getInstance().nextId();
        Job job = new Job(id, jobFromYAML.toCommitTransformJobReq(-1), trigger);
        job.setMetaStored(true);
        long jobId = commitJob(job);
        if (jobId < 0) {
          LOGGER.error("Cannot initialize job, jobId is less than 0");
        }
      } catch (IOException e) {
        LOGGER.error("Cannot read yaml file: {}", path);
      }
    }
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
      if (job.isScheduled() && !job.isMetaStored()) {
        // save the trigger in meta and job as yaml
        saveJobAsYaml(job.getJobId());
        metaManager.storeTransformJob(getLocalTransformJobMeta(job.getJobId()));
      }
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
  }

  private void process(Job job) throws Exception {
    JobRunner runner = new JobRunner(job);
    job.setStartTime(System.currentTimeMillis());
    try {
      runner.start();
      jobRunnerMap.put(job.getJobId(), runner);
      runner.run();
    } catch (Exception e) {
      LOGGER.error("Fail to process transform job id={}, because", job.getJobId(), e);
      throw e;
    }
    // TODO: should we set end time and log time cost for failed jobs?
    if (!runner.scheduled()) {
      job.setEndTime(System.currentTimeMillis());
      LOGGER.info("Job id={} cost {} ms.", job.getJobId(), job.getEndTime() - job.getStartTime());
    }
  }

  public boolean cancel(long jobId) throws TransformException {
    Job job = jobMap.get(jobId);
    if (job == null) {
      throw new TransformException("Job with id: " + jobId + " is not found.");
    }
    JobRunner runner = jobRunnerMap.get(jobId);
    if (runner == null) {
      // job finished/failed/closed
      JobState jobState = queryJobState(jobId);
      String err;
      switch (jobState) {
        case JOB_FINISHED:
          err = "Job with id: " + jobId + " has finished.";
          break;
        case JOB_FAILED:
          err = "Job with id: " + jobId + " has failed.";
          break;
        case JOB_CLOSED:
          err = "Job with id: " + jobId + " has closed.";
          break;
        default:
          err =
              "Runner of unfinished job with id: "
                  + jobId
                  + " is not found. Please check server log.";
          break;
      }
      throw new TransformException(err);
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
        // atomic guard
        if (!job.getActive().compareAndSet(true, false)) {
          throw new TransformException("Cannot set active status of job with id: " + jobId + ".");
        }
      case JOB_PARTIALLY_FAILING:
        // wait for job to finish clean up
        try {
          long waitTime = 10 * 1000; // 10 seconds max waiting time
          while (job.getState() == JobState.JOB_PARTIALLY_FAILING && waitTime > 0) {
            waitTime -= 1000;
            Thread.sleep(1000);
          }
          if (job.getState() == JobState.JOB_PARTIALLY_FAILED) {
            throw new TransformException(
                "One execution of job with id: "
                    + jobId
                    + " is still failing after 10 seconds. Please check server.");
          }
        } catch (InterruptedException e) {
          throw new TransformException(
              "One execution of job with id: "
                  + jobId
                  + " is failing or failed yet cannot be removed. Please check server.",
              e);
        }
      case JOB_PARTIALLY_FAILED:
      case JOB_IDLE:
        // reorder as Normal run: [set-ING,] close, set-ED, remove[, set end time, log time cost].
        job.setState(JobState.JOB_CLOSING);
        runner.close();
        job.setState(JobState.JOB_CLOSED);
        removeJob(jobId);
        job.setEndTime(System.currentTimeMillis());
        LOGGER.info("Job id={} cost {} ms.", job.getJobId(), job.getEndTime() - job.getStartTime());
        return true;
      case JOB_FAILED:
      case JOB_FAILING:
        try {
          removeFailedScheduleJob(jobId);
        } catch (Exception e) {
          throw new TransformException(
              "Job with id: " + jobId + " is failing or failed yet cannot be removed.", e);
        }
        // automatically remove failed tasks, but throw warning
        throw new TransformException("Job had failed and was removed.");
      case JOB_CLOSING:
      case JOB_CLOSED:
        // this job had received cancellation request and didn't finish yet.
        // finish: job runner should be null
        throw new TransformException("Job is still closing. Please query again later.");
      default:
        return false;
    }
  }

  /** 当任务的所有执行周期都结束，将其信息删除 */
  public void removeFinishedScheduleJob(long jobId) throws TransformException {
    Pair<Job, JobRunner> pair = getJobAndRunner(jobId);
    Job job = pair.k;
    JobRunner runner = pair.v;
    if (job.getState() == JobState.JOB_FINISHED) {
      removeJob(jobId);
      runner.close();
      return;
    }
    throw new TransformException(
        "Job with id: " + jobId + "did not finish correctly. Current state: " + job.getState());
  }

  public void removeFailedScheduleJob(long jobId) throws TransformException, InterruptedException {
    Pair<Job, JobRunner> pair = getJobAndRunner(jobId);
    Job job = pair.k;
    JobRunner runner = pair.v;
    long waitTime = 10 * 1000; // 10 seconds max waiting time
    while (job.getState() == JobState.JOB_FAILING && waitTime > 0) {
      waitTime -= 1000;
      Thread.sleep(1000);
    }
    if (job.getState() == JobState.JOB_FAILING) {
      throw new TransformException(
          "Job with id: " + jobId + " is still failing after 10 seconds. Please check server.");
    }
    if (job.getState() == JobState.JOB_FAILED) {
      removeJob(jobId);
      runner.close();
    } else {
      throw new TransformException(
          "Job with id: " + jobId + "is supposed to be FAILED. Current state: " + job.getState());
    }
  }

  private Pair<Job, JobRunner> getJobAndRunner(long jobId) throws TransformException {
    Job job = jobMap.get(jobId);
    if (job == null) {
      throw new TransformException("No job with id: " + jobId + " exists.");
    }
    JobRunner runner = jobRunnerMap.get(jobId);
    if (runner == null) {
      throw new TransformException("No job runner with id: " + jobId + " exists.");
    }
    return new Pair<>(job, runner);
  }

  public JobState queryJobState(long jobId) {
    if (jobMap.containsKey(jobId)) {
      return jobMap.get(jobId).getState();
    } else {
      return null;
    }
  }

  public HashMap<JobState, List<Long>> showEligibleJob(JobState jobState) {
    // jobState = null: show all jobs
    HashMap<JobState, List<Long>> jobStateMap = new HashMap<>();
    for (Job job : jobMap.values()) {
      if (jobState == null || job.getState().equals(jobState)) {
        if (!jobStateMap.containsKey(job.getState())) {
          jobStateMap.put(job.getState(), new ArrayList<>());
        }
        jobStateMap.get(job.getState()).add(job.getJobId());
      }
    }
    return jobStateMap;
  }

  public boolean isRegisterTaskRunning(String taskName) {
    for (Job job : jobMap.values()) {
      JobState jobState = job.getState();
      if (jobState.equals(JobState.JOB_RUNNING) || jobState.equals(JobState.JOB_CREATED)) {
        for (Task task : job.getTaskList()) {
          if (task.getTaskType().equals(TaskType.PYTHON)) {
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

  private void removeJob(long jobId) {
    jobRunnerMap.remove(jobId);

    Job job = jobMap.get(jobId);
    if (job.isScheduled()) {
      String path = String.join(File.separator, JobYamlDir, job.getName() + ".yaml");
      try {
        metaManager.dropTransformJob(job.getName());
        Files.deleteIfExists(Paths.get(path));
      } catch (IOException e) {
        LOGGER.error("Cannot delete yaml file {}", path, e);
      }
    }
  }

  public void saveJobAsYaml(long jobId) {
    Job job = jobMap.get(jobId);
    String yamlFileName = String.join(File.separator, JobYamlDir, jobId + ".yaml");
    YAMLWriter writer = new YAMLWriter();
    try {
      LOGGER.debug(
          "Writing job {} into yaml:{}.", jobId, new File(yamlFileName).getCanonicalPath());
      writer.writeJobIntoYAML(new File(yamlFileName), job.toYaml());
    } catch (IOException e) {
      LOGGER.error("Cannot write yaml file {}", yamlFileName, e);
    }
  }

  public TransformJobMeta getLocalTransformJobMeta(long jobId) {
    Job job = jobMap.get(jobId);
    return new TransformJobMeta(
        job.getName(), toTriggerDescriptor(job.getTrigger()), config.getIp(), config.getPort());
  }
}
