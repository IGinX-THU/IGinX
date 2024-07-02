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
import cn.edu.tsinghua.iginx.thrift.AddStorageEnginesReq;
import cn.edu.tsinghua.iginx.thrift.StorageEngine;
import java.util.ArrayList;
import java.util.List;

public class AddStorageEngineStatement extends SystemStatement {

  private final List<StorageEngine> engines;

  public AddStorageEngineStatement() {
    engines = new ArrayList<>();
    this.statementType = StatementType.ADD_STORAGE_ENGINE;
  }

  public List<StorageEngine> getEngines() {
    return engines;
  }

  public void setEngines(StorageEngine engine) {
    this.engines.add(engine);
  }

  @Override
  public void execute(RequestContext ctx) {
    IginxWorker worker = IginxWorker.getInstance();
    AddStorageEnginesReq req = new AddStorageEnginesReq(ctx.getSessionId(), engines);
    ctx.setResult(new Result(worker.addStorageEngines(req)));
  }
}
