package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.IginxWorker;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.engine.shared.exception.StatementExecutionException;
import cn.edu.tsinghua.iginx.engine.shared.function.manager.FunctionManager;
import cn.edu.tsinghua.iginx.thrift.DropTaskReq;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DropTaskStatement extends SystemStatement {

  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory.getLogger(DropTaskStatement.class);

  private final String name;

  private final IginxWorker worker = IginxWorker.getInstance();

  private static final FunctionManager functionManager = FunctionManager.getInstance();

  public DropTaskStatement(String name) {
    this.statementType = StatementType.DROP_TASK;
    this.name = name;
  }

  @Override
  public void execute(RequestContext ctx) throws StatementExecutionException {
    DropTaskReq req = new DropTaskReq(ctx.getSessionId(), name);
    Status status = worker.dropTask(req);
    if (status.code == RpcUtils.SUCCESS.code) {
      functionManager.removeFunction(name.trim());
    }
    ctx.setResult(new Result(status));
  }
}
