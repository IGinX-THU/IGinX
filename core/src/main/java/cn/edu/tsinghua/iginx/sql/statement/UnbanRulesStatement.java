package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.IginxWorker;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.thrift.UnbanRulesReq;
import java.util.List;

public class UnbanRulesStatement extends SystemStatement {

  private final IginxWorker worker = IginxWorker.getInstance();

  private final List<String> rules;

  public UnbanRulesStatement(List<String> rules) {
    this.statementType = StatementType.UNBAN_RULES;
    this.rules = rules;
  }

  @Override
  public void execute(RequestContext ctx) throws ExecutionException {
    UnbanRulesReq req = new UnbanRulesReq(ctx.getSessionId(), rules);
    Status status = worker.unbanRules(req);

    Result result = new Result(status);
    ctx.setResult(result);
  }
}
