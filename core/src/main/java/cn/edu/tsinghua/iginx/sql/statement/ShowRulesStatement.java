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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.exception.StatementExecutionException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.ShowRulesReq;
import cn.edu.tsinghua.iginx.thrift.ShowRulesResp;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class ShowRulesStatement extends SystemStatement {

  private final IginxWorker worker = IginxWorker.getInstance();

  public ShowRulesStatement() {
    this.statementType = StatementType.SHOW_RULES;
  }

  @Override
  public void execute(RequestContext ctx) throws StatementExecutionException {
    ShowRulesReq req = new ShowRulesReq(ctx.getSessionId());
    ShowRulesResp resp = worker.showRules(req);
    Map<String, Boolean> rules = resp.getRules();

    Result result = new Result(resp.status);
    if (ctx.isUseStream()) {
      Header header =
          new Header(
              Arrays.asList(
                  new Field("Rules", DataType.BINARY), new Field("Status", DataType.BINARY)));
      List<Row> rowList = new ArrayList<>();
      rules.forEach(
          (ruleName, ruleStatus) ->
              rowList.add(
                  new Row(
                      header,
                      new Object[] {
                        ruleName.getBytes(StandardCharsets.UTF_8),
                        ruleStatus
                            ? "ON".getBytes(StandardCharsets.UTF_8)
                            : "OFF".getBytes(StandardCharsets.UTF_8)
                      })));
      RowStream table = new Table(header, rowList);
      result.setResultStream(table);
    } else {
      result.setRules(rules);
    }
    ctx.setResult(result);
  }
}
