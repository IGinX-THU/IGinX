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
