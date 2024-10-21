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
