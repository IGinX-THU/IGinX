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
import cn.edu.tsinghua.iginx.thrift.SetRulesReq;
import cn.edu.tsinghua.iginx.thrift.Status;
import java.util.Map;

public class SetRulesStatement extends SystemStatement {

  private final IginxWorker worker = IginxWorker.getInstance();

  private final Map<String, Boolean> rulesChange;

  public SetRulesStatement(Map<String, Boolean> rulesChange) {
    this.statementType = StatementType.SET_RULES;
    this.rulesChange = rulesChange;
  }

  @Override
  public void execute(RequestContext ctx) throws StatementExecutionException {
    SetRulesReq req = new SetRulesReq(ctx.getSessionId(), rulesChange);
    Status status = worker.setRules(req);

    Result result = new Result(status);
    ctx.setResult(result);
  }
}
