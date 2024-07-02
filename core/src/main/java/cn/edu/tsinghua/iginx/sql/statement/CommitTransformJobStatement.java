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

import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.engine.shared.exception.StatementExecutionException;
import cn.edu.tsinghua.iginx.transform.exec.TransformJobManager;
import cn.edu.tsinghua.iginx.transform.pojo.Job;
import cn.edu.tsinghua.iginx.utils.JobFromYAML;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import cn.edu.tsinghua.iginx.utils.SnowFlakeUtils;
import cn.edu.tsinghua.iginx.utils.YAMLReader;
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
    try {
      YAMLReader reader = new YAMLReader(path);
      JobFromYAML jobFromYAML = reader.getJobFromYAML();

      long id = SnowFlakeUtils.getInstance().nextId();
      Job job = new Job(id, ctx.getSessionId(), jobFromYAML);

      long jobId = manager.commitJob(job);
      if (jobId < 0) {
        ctx.setResult(new Result(RpcUtils.FAILURE));
      } else {
        Result result = new Result(RpcUtils.SUCCESS);
        result.setJobId(jobId);
        ctx.setResult(result);
      }
    } catch (FileNotFoundException e) {
      ctx.setResult(new Result(RpcUtils.FAILURE));
    }
  }
}
