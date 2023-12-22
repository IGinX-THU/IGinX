package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.IginxWorker;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.ShowRulesReq;
import cn.edu.tsinghua.iginx.thrift.ShowRulesResp;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ShowRulesStatement extends SystemStatement {

  private final IginxWorker worker = IginxWorker.getInstance();

  public ShowRulesStatement() {
    this.statementType = StatementType.SHOW_RULES;
  }

  @Override
  public void execute(RequestContext ctx) throws ExecutionException {
    ShowRulesReq req = new ShowRulesReq(ctx.getSessionId());
    ShowRulesResp resp = worker.showRules(req);
    List<String> rules = resp.getRules();

    Result result = new Result(resp.status);
    if (ctx.isUseStream()) {
      Header header = new Header(Collections.singletonList(new Field("Rules", DataType.BINARY)));
      List<Row> rowList = new ArrayList<>();
      rules.forEach(
          rule ->
              rowList.add(new Row(header, new Object[] {rule.getBytes(StandardCharsets.UTF_8)})));
      RowStream table = new Table(header, rowList);
      result.setResultStream(table);
    } else {
      result.setRules(rules);
    }
    ctx.setResult(result);
  }
}
