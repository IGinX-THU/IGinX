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
package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.IginxWorker;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.engine.shared.exception.StatementExecutionException;
import cn.edu.tsinghua.iginx.thrift.JobState;
import cn.edu.tsinghua.iginx.thrift.QueryTransformJobStatusReq;
import cn.edu.tsinghua.iginx.thrift.QueryTransformJobStatusResp;
import cn.edu.tsinghua.iginx.utils.RpcUtils;

public class ShowJobStatusStatement extends SystemStatement {

  private final long jobId;

  private final IginxWorker worker = IginxWorker.getInstance();

  public ShowJobStatusStatement(long jobId) {
    this.statementType = StatementType.SHOW_JOB_STATUS;
    this.jobId = jobId;
  }

  @Override
  public void execute(RequestContext ctx) throws StatementExecutionException {
    QueryTransformJobStatusReq req = new QueryTransformJobStatusReq(ctx.getSessionId(), jobId);
    QueryTransformJobStatusResp resp = worker.queryTransformJobStatus(req);
    JobState jobState = resp.getJobState();

    Result result = new Result(RpcUtils.SUCCESS);
    result.setJobState(jobState);
    ctx.setResult(result);
  }
}
