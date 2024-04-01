package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.IginxWorker;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.engine.shared.exception.StatementExecutionException;
import cn.edu.tsinghua.iginx.thrift.RegisterTaskReq;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.thrift.UDFType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RegisterTaskStatement extends SystemStatement {

  private final String name;

  private final String filePath;

  private final String className;

  private final List<UDFType> types;

  private final IginxWorker worker = IginxWorker.getInstance();

  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(RegisterTaskStatement.class);

  public RegisterTaskStatement(String name, String filePath, String className, List<UDFType> types) {
    this.statementType = StatementType.REGISTER_TASK;
    this.name = name;
    this.filePath = filePath;
    this.className = className;
    this.types = types;
  }

  @Override
  public void execute(RequestContext ctx) throws StatementExecutionException {
    RegisterTaskReq req = new RegisterTaskReq(ctx.getSessionId(), name, filePath, className, types);
    Status status = worker.registerTask(req);
    ctx.setResult(new Result(status));
  }
}
