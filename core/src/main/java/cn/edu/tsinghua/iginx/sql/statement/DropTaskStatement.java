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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.IginxWorker;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.engine.shared.exception.StatementExecutionException;
import cn.edu.tsinghua.iginx.thrift.DropTaskReq;
import cn.edu.tsinghua.iginx.thrift.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DropTaskStatement extends SystemStatement {

  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory.getLogger(DropTaskStatement.class);

  private final String name;

  private final IginxWorker worker = IginxWorker.getInstance();

  public DropTaskStatement(String name) {
    this.statementType = StatementType.DROP_TASK;
    this.name = name;
  }

  @Override
  public void execute(RequestContext ctx) throws StatementExecutionException {
    DropTaskReq req = new DropTaskReq(ctx.getSessionId(), name);
    Status status = worker.dropTask(req);
    ctx.setResult(new Result(status));
  }
}
