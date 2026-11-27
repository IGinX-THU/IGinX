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
package cn.edu.tsinghua.iginx.engine.shared;

import static org.junit.Assert.assertEquals;

import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.ExecuteStatementResp;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import java.util.Arrays;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.junit.Test;

public class ResultResponseTest {
  private void roundTrip(Result result, SqlType expectedType) throws Exception {
    for (TProtocolFactory protocol :
        Arrays.asList(new TBinaryProtocol.Factory(), new TCompactProtocol.Factory())) {
      ExecuteSqlResp sql = new ExecuteSqlResp();
      new TDeserializer(protocol)
          .deserialize(sql, new TSerializer(protocol).serialize(result.getExecuteSqlResp()));
      assertEquals(result.getStatus(), sql.getStatus());
      assertEquals(expectedType, sql.getType());

      ExecuteStatementResp statement = new ExecuteStatementResp();
      new TDeserializer(protocol)
          .deserialize(
              statement, new TSerializer(protocol).serialize(result.getExecuteStatementResp(1)));
      assertEquals(result.getStatus(), statement.getStatus());
      assertEquals(expectedType, statement.getType());
    }
  }

  @Test
  public void earlyAccessDenialSurvivesThriftRoundTrip() throws Exception {
    // Revocation between the RPC entry check and the executor check returns before SQL parsing.
    roundTrip(new Result(RpcUtils.ACCESS_DENY), SqlType.Unknown);
  }

  @Test
  public void earlyServiceErrorsSurviveThriftRoundTrip() throws Exception {
    roundTrip(new Result(RpcUtils.SERVICE_UNAVAILABLE), SqlType.Unknown);
    roundTrip(new Result(RpcUtils.FAILURE), SqlType.Unknown);
  }

  @Test
  public void parsedStatementTypeIsPreserved() throws Exception {
    Result success = new Result(RpcUtils.SUCCESS);
    success.setSqlType(SqlType.Insert);
    roundTrip(success, SqlType.Insert);
    Result denied = new Result(RpcUtils.ACCESS_DENY);
    denied.setSqlType(SqlType.Query);
    roundTrip(denied, SqlType.Query);
  }
}
