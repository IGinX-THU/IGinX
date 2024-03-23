package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.compaction.CompactionManager;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactStatement extends SystemStatement {
  private static final Logger LOGGER = LoggerFactory.getLogger(CompactStatement.class);

  public CompactStatement() {
    this.statementType = StatementType.COMPACT;
  }

  @Override
  public void execute(RequestContext ctx) {
    Result result = new Result(RpcUtils.SUCCESS);
    try {
      CompactionManager.getInstance().clearFragment();
      ctx.setResult(result);
    } catch (Exception e) {
      LOGGER.error("execute compact failed", e);
      ctx.setResult(new Result(RpcUtils.FAILURE));
    }
  }
}
