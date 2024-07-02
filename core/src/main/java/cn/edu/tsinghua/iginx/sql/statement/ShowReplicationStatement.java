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

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import java.util.Collections;

public class ShowReplicationStatement extends SystemStatement {

  public ShowReplicationStatement() {
    this.statementType = StatementType.SHOW_REPLICATION;
  }

  @Override
  public void execute(RequestContext ctx) {
    Result result = new Result(RpcUtils.SUCCESS);

    int num = ConfigDescriptor.getInstance().getConfig().getReplicaNum() + 1;

    if (ctx.isUseStream()) {
      Header header = new Header(Collections.singletonList(new Field("replica", DataType.INTEGER)));
      RowStream table =
          new Table(header, Collections.singletonList(new Row(header, new Integer[] {num})));
      result.setResultStream(table);
    } else {
      result.setReplicaNum(num);
    }
    ctx.setResult(result);
  }
}
