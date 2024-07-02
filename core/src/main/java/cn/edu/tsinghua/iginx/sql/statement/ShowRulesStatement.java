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
