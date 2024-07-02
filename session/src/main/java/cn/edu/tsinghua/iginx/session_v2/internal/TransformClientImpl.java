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
      if (taskType.equals(TaskType.IginX)) {
        taskInfo.setSqlList(task.getSqlList());
      } else if (taskType.equals(TaskType.Python)) {
        taskInfo.setPyTaskName(task.getPyTaskName());
      }
      taskInfoList.add(taskInfo);
    }

    CommitTransformJobReq req =
        new CommitTransformJobReq(sessionId, taskInfoList, transform.getExportType());
    if (transform.getExportType().equals(ExportType.File)) {
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
