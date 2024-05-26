package cn.edu.tsinghua.iginx.transform.exec;

import cn.edu.tsinghua.iginx.thrift.DataFlowType;
import cn.edu.tsinghua.iginx.thrift.JobState;
import cn.edu.tsinghua.iginx.transform.api.Runner;
import cn.edu.tsinghua.iginx.transform.api.Stage;
import cn.edu.tsinghua.iginx.transform.exception.TransformException;
import cn.edu.tsinghua.iginx.transform.exception.UnknownDataFlowException;
import cn.edu.tsinghua.iginx.transform.pojo.BatchStage;
import cn.edu.tsinghua.iginx.transform.pojo.Job;
import cn.edu.tsinghua.iginx.transform.pojo.StreamStage;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobRunner implements Runner {
  private static final Logger LOGGER = LoggerFactory.getLogger(JobRunner.class);

  private final Job job;

  private final List<Runner> runnerList;

  public JobRunner(Job job) {
    this.job = job;
    this.runnerList = new ArrayList<>();
  }

  @Override
  public void start() throws UnknownDataFlowException {
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
  }

  @Override
  public void run() {
    job.setState(JobState.JOB_RUNNING);
    try {
      for (Runner runner : runnerList) {
        runner.start();
        runner.run();
        runner.close();
      }
      // we don't need this.close() because all children runners are closed.
      if (job.getActive().compareAndSet(true, false)) {
        job.setState(JobState.JOB_FINISHED);
        job.setException(null);
      }
    } catch (TransformException e) {
      LOGGER.error("Fail to run transform job id={}, because", job.getJobId(), e);
      if (job.getActive().compareAndSet(true, false)) {
        job.setState(JobState.JOB_FAILING);
        job.setException(e);
        close();
        job.setState(JobState.JOB_FAILED);
      }
    }
  }

  @Override
  public void close() {
    try {
      for (Runner runner : runnerList) {
        runner.close();
      }
    } catch (TransformException e) {
      LOGGER.error("Fail to close Transform job runner id={}, because", job.getJobId(), e);
    }
  }
}
