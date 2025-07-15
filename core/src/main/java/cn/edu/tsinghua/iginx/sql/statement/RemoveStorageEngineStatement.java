/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.IginxWorker;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.engine.shared.exception.StatementExecutionException;
import cn.edu.tsinghua.iginx.thrift.RemoveStorageEngineReq;
import cn.edu.tsinghua.iginx.thrift.RemovedStorageEngineInfo;
import java.util.ArrayList;
import java.util.List;

public class RemoveStorageEngineStatement extends SystemStatement {

  private List<RemovedStorageEngineInfo> storageEngineList;

  private boolean isForAllIginx;

  public List<RemovedStorageEngineInfo> getStorageEngineList() {
    return storageEngineList;
  }

  public void setStorageEngineList(List<RemovedStorageEngineInfo> storageEngineList) {
    this.storageEngineList = storageEngineList;
  }

  public void addStorageEngine(RemovedStorageEngineInfo storageEngine) {
    this.storageEngineList.add(storageEngine);
  }

  public boolean isForAllIginx() {
    return isForAllIginx;
  }

  public void setForAllIginx(boolean isForAllIginx) {
    this.isForAllIginx = isForAllIginx;
  }

  public RemoveStorageEngineStatement() {
    storageEngineList = new ArrayList<>();
    this.statementType = StatementType.REMOVE_HISTORY_DATA_SOURCE;
  }

  @Override
  public void execute(RequestContext ctx) throws StatementExecutionException {
    IginxWorker worker = IginxWorker.getInstance();
    RemoveStorageEngineReq req =
        new RemoveStorageEngineReq(ctx.getSessionId(), storageEngineList, isForAllIginx);
    ctx.setResult(new Result(worker.removeStorageEngine(req)));
  }
}
