package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.IginxWorker;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.thrift.BanRulesReq;
import cn.edu.tsinghua.iginx.thrift.Status;
import java.util.List;

public class BanRulesStatement extends SystemStatement {

  private final IginxWorker worker = IginxWorker.getInstance();

  private final List<String> rules;

  public BanRulesStatement(List<String> rules) {
    this.statementType = StatementType.BAN_RULES;
    this.rules = rules;
  }

  @Override
  public void execute(RequestContext ctx) throws ExecutionException {
    BanRulesReq req = new BanRulesReq(ctx.getSessionId(), rules);
    Status status = worker.banRules(req);

    Result result = new Result(status);
    ctx.setResult(result);
  }
}
