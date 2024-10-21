package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.IginxWorker;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.engine.shared.exception.StatementExecutionException;
import cn.edu.tsinghua.iginx.thrift.DropPythonModuleReq;
import cn.edu.tsinghua.iginx.thrift.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DropPythonModuleStatement extends SystemStatement {

  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory.getLogger(DropPythonModuleStatement.class);

  private final String name;

  private final IginxWorker worker = IginxWorker.getInstance();

  public DropPythonModuleStatement(String name) {
    this.name = name;
  }

  @Override
  public void execute(RequestContext ctx) throws StatementExecutionException {
    DropPythonModuleReq req = new DropPythonModuleReq(ctx.getSessionId(), name);
    Status status = worker.dropPythonModule(req);
    ctx.setResult(new Result(status));
  }
}
