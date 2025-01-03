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
import cn.edu.tsinghua.iginx.utils.RpcUtils;

public class CommitTransformJobStatement extends SystemStatement {

  private final String path;

  private final TransformJobManager manager = TransformJobManager.getInstance();

  public CommitTransformJobStatement(String path) {
    this.statementType = StatementType.COMMIT_TRANSFORM_JOB;
    this.path = path;
  }

  @Override
  public void execute(RequestContext ctx) throws StatementExecutionException {
    // this statement will only resolve file path and return to client.
    // client will read job information from file afterward.
    ctx.setResult(new Result(RpcUtils.SUCCESS));
    ctx.getResult().setJobYamlPath(path);
  }
}
