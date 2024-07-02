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
package cn.edu.tsinghua.iginx.transform.data;

import cn.edu.tsinghua.iginx.engine.ContextBuilder;
import cn.edu.tsinghua.iginx.engine.StatementExecutor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.thrift.ExecuteStatementReq;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IginXWriter extends ExportWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(IginXWriter.class);

  private final long sessionId;

  private final StatementExecutor executor = StatementExecutor.getInstance();

  private final ContextBuilder contextBuilder = ContextBuilder.getInstance();

  public IginXWriter(long sessionId) {
    this.sessionId = sessionId;
  }

  @Override
  public void write(BatchData batchData) {
    String insertSQL = buildSQL(batchData);
    LOGGER.info("Insert statement: {}", insertSQL);

    if (!insertSQL.equals("")) {
      ExecuteStatementReq req = new ExecuteStatementReq(sessionId, insertSQL);
      RequestContext context = contextBuilder.build(req);
      executor.execute(context);
    } else {
      LOGGER.error("Fail to execute insert statement.");
    }
  }

  private String buildSQL(BatchData batchData) {
    StringBuilder builder = new StringBuilder();

    // construct paths
    builder.append("INSERT INTO transform(key, ");
    Header header = batchData.getHeader();
    header
        .getFields()
        .forEach(field -> builder.append(reformatPath(field.getFullName())).append(","));
    builder.deleteCharAt(builder.length() - 1);

    // construct values
    builder.append(") VALUES");
    // use System.nanoTime() to avoid timestamp mistake on windows runner in action
    long index = System.nanoTime();
    for (Row row : batchData.getRowList()) {
      builder.append(" (");
      builder.append(index).append(",");
      for (Object value : row.getValues()) {
        builder.append(value).append(",");
      }
      builder.deleteCharAt(builder.length() - 1);
      builder.append("),");
      index++;
    }
    builder.deleteCharAt(builder.length() - 1).append(";");
    return builder.toString();
  }

  private long getCurrentTimeInNS() {
    Instant now = Instant.now();
    return now.getEpochSecond() * 1_000_000_000L + now.getNano();
  }

  private String reformatPath(String path) {
    if (!path.contains("(") && !path.contains(")")) return path;
    path = path.replaceAll("[{]", "[");
    path = path.replaceAll("[}]", "]");
    return path;
  }
}
