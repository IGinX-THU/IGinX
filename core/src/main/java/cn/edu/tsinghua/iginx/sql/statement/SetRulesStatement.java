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
