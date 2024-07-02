/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
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
