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
package cn.edu.tsinghua.iginx.engine.shared;

import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.utils.SnowFlakeUtils;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;

@Data
public class RequestContext {

  private long id;

  private long startTime;

  private long endTime;

  private long sessionId;

  private Map<String, Object> extraParams;

  private Status status;

  private String sql;

  private boolean fromSQL;

  private SqlType sqlType;

  private Statement statement;

  private Result result;

  private boolean useStream;

  private PhysicalTask physicalTree;

  private ByteBuffer loadCSVFileByteBuffer;

  private ByteBuffer UDFModuleByteBuffer;

  private boolean isRemoteUDF;

  private String warningMsg;

  private void init() {
    this.id = SnowFlakeUtils.getInstance().nextId();
    this.startTime = System.currentTimeMillis();
    this.extraParams = new HashMap<>();
  }

  public RequestContext() {
    init();
  }

  public RequestContext(long sessionId) {
    init();
    this.sessionId = sessionId;
  }

  public RequestContext(long sessionId, Statement statement) {
    this(sessionId, statement, false);
  }

  public RequestContext(long sessionId, Statement statement, boolean useStream) {
    init();
    this.sessionId = sessionId;
    this.statement = statement;
    this.fromSQL = false;
    this.useStream = useStream;
  }

  public RequestContext(long sessionId, String sql) {
    this(sessionId, sql, false);
  }

  public RequestContext(long sessionId, String sql, boolean useStream) {
    init();
    this.sessionId = sessionId;
    this.sql = sql;
    this.fromSQL = true;
    this.sqlType = SqlType.Unknown;
    this.useStream = useStream;
  }

  public Object getExtraParam(String key) {
    return extraParams.getOrDefault(key, null);
  }

  public void setExtraParam(String key, Object value) {
    extraParams.put(key, value);
  }

  public boolean isUseStream() {
    return useStream;
  }

  public void setResult(Result result) {
    this.result = result;
    if (this.result != null) {
      this.result.setQueryId(id);
    }
    this.endTime = System.currentTimeMillis();
  }

  public void setWarningMsg(String warningMsg) {
    this.warningMsg = warningMsg;
  }

  public String getWarningMsg() {
    return warningMsg;
  }
}
