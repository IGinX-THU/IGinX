package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.IginxWorker;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.engine.shared.exception.StatementExecutionException;
import cn.edu.tsinghua.iginx.thrift.RemoveHistoryDataSourceReq;
import cn.edu.tsinghua.iginx.thrift.RemovedStorageEngineInfo;
import java.util.ArrayList;
import java.util.List;

public class RemoveHistoryDataSourceStatement extends SystemStatement {

  private List<RemovedStorageEngineInfo> storageEngineList;

  public List<RemovedStorageEngineInfo> getStorageEngineList() {
    return storageEngineList;
  }

  public void setStorageEngineList(List<RemovedStorageEngineInfo> storageEngineList) {
    this.storageEngineList = storageEngineList;
  }

  public void addStorageEngine(RemovedStorageEngineInfo storageEngine) {
    this.storageEngineList.add(storageEngine);
  }

  public RemoveHistoryDataSourceStatement() {
    storageEngineList = new ArrayList<>();
    this.statementType = StatementType.REMOVE_HISTORY_DATA_SOURCE;
  }

  @Override
  public void execute(RequestContext ctx) throws StatementExecutionException {
    IginxWorker worker = IginxWorker.getInstance();
    RemoveHistoryDataSourceReq req =
        new RemoveHistoryDataSourceReq(ctx.getSessionId(), storageEngineList);
    ctx.setResult(new Result(worker.removeHistoryDataSource(req)));
  }
}
