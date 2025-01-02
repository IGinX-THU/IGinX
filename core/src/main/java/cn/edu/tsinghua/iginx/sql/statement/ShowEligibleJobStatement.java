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
package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.IginxWorker;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.engine.shared.exception.StatementExecutionException;
import cn.edu.tsinghua.iginx.thrift.JobState;
import cn.edu.tsinghua.iginx.thrift.ShowEligibleJobReq;
import cn.edu.tsinghua.iginx.thrift.ShowEligibleJobResp;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import java.util.List;
import java.util.Map;

public class ShowEligibleJobStatement extends SystemStatement {

  private final JobState jobState;

  private final IginxWorker worker = IginxWorker.getInstance();

  public ShowEligibleJobStatement(JobState jobState) {
    this.statementType = StatementType.SHOW_ELIGIBLE_JOB;
    this.jobState = jobState;
  }

  @Override
  public void execute(RequestContext ctx) throws StatementExecutionException {
    ShowEligibleJobReq req = new ShowEligibleJobReq(ctx.getSessionId()).setJobState(jobState);
    ShowEligibleJobResp resp = worker.showEligibleJob(req);
    Map<JobState, List<Long>> jobStateMap = resp.getJobStateMap();

    Result result = new Result(RpcUtils.SUCCESS);
    result.setJobStateMap(jobStateMap);
    ctx.setResult(result);
  }
}
