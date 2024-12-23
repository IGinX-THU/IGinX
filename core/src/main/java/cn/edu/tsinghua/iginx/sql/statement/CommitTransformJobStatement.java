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

import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.engine.shared.exception.StatementExecutionException;
import cn.edu.tsinghua.iginx.transform.exec.TransformJobManager;
import cn.edu.tsinghua.iginx.transform.pojo.Job;
import cn.edu.tsinghua.iginx.utils.JobFromYAML;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import cn.edu.tsinghua.iginx.utils.SnowFlakeUtils;
import cn.edu.tsinghua.iginx.utils.YAMLReader;
import java.io.File;
import java.io.FileNotFoundException;

public class CommitTransformJobStatement extends SystemStatement {

  private final String path;

  private final TransformJobManager manager = TransformJobManager.getInstance();

  public CommitTransformJobStatement(String path) {
    this.statementType = StatementType.COMMIT_TRANSFORM_JOB;
    this.path = path;
  }

  @Override
  public void execute(RequestContext ctx) throws StatementExecutionException {
    File file = new File(path);
    if (ctx.isRemoteSession() || !file.isAbsolute()) {
      ctx.setResult(new Result(RpcUtils.SUCCESS));
      ctx.getResult().setJobYamlPath(path);
      return;
    }
    try {
      YAMLReader reader = new YAMLReader(path);
      JobFromYAML jobFromYAML = reader.getJobFromYAML();

      long id = SnowFlakeUtils.getInstance().nextId();
      Job job = new Job(id, jobFromYAML.toCommitTransformJobReq(ctx.getSessionId()));

      long jobId = manager.commitJob(job);
      if (jobId < 0) {
        ctx.setResult(new Result(RpcUtils.FAILURE));
      } else {
        Result result = new Result(RpcUtils.SUCCESS);
        result.setJobId(jobId);
        ctx.setResult(result);
      }
    } catch (FileNotFoundException e) {
      String errMsg = "Cannot find file in path:" + path;
      ctx.setResult(new Result(RpcUtils.ErrorStatus(errMsg)));
    }
  }
}
