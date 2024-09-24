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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.GetClusterInfoReq;
import cn.edu.tsinghua.iginx.thrift.GetClusterInfoResp;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ShowClusterInfoStatement extends SystemStatement {

  public ShowClusterInfoStatement() {
    this.statementType = StatementType.SHOW_CLUSTER_INFO;
  }

  @Override
  public void execute(RequestContext ctx) {
    IginxWorker worker = IginxWorker.getInstance();
    GetClusterInfoReq req = new GetClusterInfoReq(ctx.getSessionId());
    GetClusterInfoResp getClusterInfoResp = worker.getClusterInfo(req);

    Result result = new Result(getClusterInfoResp.getStatus());
    if (ctx.isUseStream()) {
      Header header =
          new Header(
              Arrays.asList(
                  new Field("InfoName", DataType.BINARY),
                  new Field("InfoDetails", DataType.BINARY)));
      List<Row> rowList = new ArrayList<>();

      if (getClusterInfoResp.isSetIginxInfos()) {
        getClusterInfoResp
            .getIginxInfos()
            .forEach(
                iginxInfo ->
                    rowList.add(
                        new Row(
                            header,
                            new Object[] {
                              "IginX".getBytes(StandardCharsets.UTF_8),
                              iginxInfo.toString().getBytes(StandardCharsets.UTF_8)
                            })));
      }
      if (getClusterInfoResp.isSetStorageEngineInfos()) {
        getClusterInfoResp
            .getStorageEngineInfos()
            .forEach(
                storageEngineInfo ->
                    rowList.add(
                        new Row(
                            header,
                            new Object[] {
                              "StorageEngine".getBytes(StandardCharsets.UTF_8),
                              storageEngineInfo.toString().getBytes(StandardCharsets.UTF_8)
                            })));
      }
      if (getClusterInfoResp.isSetMetaStorageInfos()) {
        getClusterInfoResp
            .getMetaStorageInfos()
            .forEach(
                metaStorageInfo ->
                    rowList.add(
                        new Row(
                            header,
                            new Object[] {
                              "MetaStorage".getBytes(StandardCharsets.UTF_8),
                              metaStorageInfo.toString().getBytes(StandardCharsets.UTF_8)
                            })));
      }
      if (getClusterInfoResp.isSetLocalMetaStorageInfo()) {
        rowList.add(
            new Row(
                header,
                new Object[] {
                  "LocalMetaStorage".getBytes(StandardCharsets.UTF_8),
                  getClusterInfoResp
                      .getLocalMetaStorageInfo()
                      .toString()
                      .getBytes(StandardCharsets.UTF_8)
                }));
      }

      RowStream table = new Table(header, rowList);
      result.setResultStream(table);
    } else {
      result.setIginxInfos(getClusterInfoResp.getIginxInfos());
      result.setStorageEngineInfos(getClusterInfoResp.getStorageEngineInfos());
      result.setMetaStorageInfos(getClusterInfoResp.getMetaStorageInfos());
      result.setLocalMetaStorageInfo(getClusterInfoResp.getLocalMetaStorageInfo());
    }
    ctx.setResult(result);
  }
}
