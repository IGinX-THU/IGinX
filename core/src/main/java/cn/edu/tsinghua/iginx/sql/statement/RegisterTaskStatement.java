package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.IginxWorker;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.engine.shared.exception.StatementExecutionException;
import cn.edu.tsinghua.iginx.thrift.RegisterTaskReq;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.thrift.UDFClassPair;
import cn.edu.tsinghua.iginx.thrift.UDFType;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegisterTaskStatement extends SystemStatement {
  @SuppressWarnings("unused")
  private static final Logger LOGGER = LoggerFactory.getLogger(RegisterTaskStatement.class);

  private final String filePath;

  private final List<UDFClassPair> pairs;

  private final List<UDFType> types;

  private final IginxWorker worker = IginxWorker.getInstance();

  public RegisterTaskStatement(String filePath, List<UDFClassPair> pairs, List<UDFType> types) {
    this.statementType = StatementType.REGISTER_TASK;
    this.pairs = pairs;
    this.filePath = filePath;
    this.types = types;
  }

  @Override
  public void execute(RequestContext ctx) throws StatementExecutionException {
    RegisterTaskReq req = new RegisterTaskReq(ctx.getSessionId(), filePath, pairs, types);
    Status status = worker.registerTask(req);
    ctx.setResult(new Result(status));
  }
}
