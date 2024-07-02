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
import cn.edu.tsinghua.iginx.thrift.ShowSessionIDReq;
import cn.edu.tsinghua.iginx.thrift.ShowSessionIDResp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ShowSessionIDStatement extends SystemStatement {

  private final IginxWorker worker = IginxWorker.getInstance();

  public ShowSessionIDStatement() {
    this.statementType = StatementType.SHOW_SESSION_ID;
  }

  @Override
  public void execute(RequestContext ctx) throws StatementExecutionException {
    ShowSessionIDReq req = new ShowSessionIDReq(ctx.getSessionId());
    ShowSessionIDResp resp = worker.showSessionID(req);
    List<Long> sessionIDs = resp.getSessionIDList();

    Result result = new Result(resp.status);
    if (ctx.isUseStream()) {
      Header header = new Header(Collections.singletonList(new Field("SessionID", DataType.LONG)));
      List<Row> rowList = new ArrayList<>();
      sessionIDs.forEach(id -> rowList.add(new Row(header, new Object[] {id})));
      RowStream table = new Table(header, rowList);
      result.setResultStream(table);
    } else {
      result.setSessionIDs(sessionIDs);
    }
    ctx.setResult(result);
  }
}
