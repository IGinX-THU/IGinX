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
package cn.edu.tsinghua.iginx.session_v2.internal;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session_v2.TransformClient;
import cn.edu.tsinghua.iginx.session_v2.domain.Task;
import cn.edu.tsinghua.iginx.session_v2.domain.Transform;
import cn.edu.tsinghua.iginx.session_v2.exception.IginXException;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.StatusUtils;
import java.util.ArrayList;
import java.util.List;
import org.apache.thrift.TException;

public class TransformClientImpl extends AbstractFunctionClient implements TransformClient {

  public TransformClientImpl(IginXClientImpl iginXClient) {
    super(iginXClient);
  }

  @Override
  public long commitTransformJob(Transform transform) {
    List<TaskInfo> taskInfoList = new ArrayList<>();
    for (Task task : transform.getTaskList()) {
      TaskType taskType = task.getTaskType();
      TaskInfo taskInfo = new TaskInfo(taskType, task.getDataFlowType());
      taskInfo.setTimeout(task.getTimeout());
      if (taskType.equals(TaskType.IGINX)) {
        taskInfo.setSqlList(task.getSqlList());
      } else if (taskType.equals(TaskType.PYTHON)) {
        taskInfo.setPyTaskName(task.getPyTaskName());
      }
      taskInfoList.add(taskInfo);
    }

    CommitTransformJobReq req =
        new CommitTransformJobReq(sessionId, taskInfoList, transform.getExportType());
    if (transform.getExportType().equals(ExportType.FILE)) {
      req.setFileName(transform.getFileName());
    }

    CommitTransformJobResp resp;

    synchronized (iginXClient) {
      iginXClient.checkIsClosed();
      try {
        resp = client.commitTransformJob(req);
        StatusUtils.verifySuccess(resp.getStatus());
      } catch (TException | SessionException e) {
        throw new IginXException("commit transform job failure: ", e);
      }
    }
    return resp.getJobId();
  }

  @Override
  public JobState queryTransformJobStatus(long jobId) {

    QueryTransformJobStatusReq req = new QueryTransformJobStatusReq(sessionId, jobId);
    QueryTransformJobStatusResp resp;

    synchronized (iginXClient) {
      iginXClient.checkIsClosed();
      try {
        resp = client.queryTransformJobStatus(req);
        StatusUtils.verifySuccess(resp.getStatus());
      } catch (TException | SessionException e) {
        throw new IginXException("query transform job status failure: ", e);
      }
    }
    return resp.getJobState();
  }

  @Override
  public void cancelTransformJob(long jobId) {

    CancelTransformJobReq req = new CancelTransformJobReq(sessionId, jobId);

    synchronized (iginXClient) {
      iginXClient.checkIsClosed();
      try {
        Status status = client.cancelTransformJob(req);
        StatusUtils.verifySuccess(status);
      } catch (TException | SessionException e) {
        throw new IginXException("cancel transform job failure: ", e);
      }
    }
  }
}
