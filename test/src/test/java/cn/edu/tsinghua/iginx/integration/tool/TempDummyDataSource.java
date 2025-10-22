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
package cn.edu.tsinghua.iginx.integration.tool;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.RemovedStorageEngineInfo;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class TempDummyDataSource implements AutoCloseable {

  public static final String DEFAULT_IP = "127.0.0.1";
  public static final int DEFAULT_PORT = 16667;
  public static final String DEFAULT_SCHEMA_PREFIX = "";
  public static final String DEFAULT_PREFIX = "";

  private final Session session;
  private final String ip;
  private final int port;
  private final StorageEngineType type;
  private final String schemaPrefix;
  private final String dataPrefix;
  private final Map<String, String> extraParams;

  public TempDummyDataSource(
      Session session, StorageEngineType type, Map<String, String> extraParams)
      throws SessionException {
    this(session, DEFAULT_PORT, type, extraParams);
  }

  public TempDummyDataSource(
      Session session, int port, StorageEngineType type, Map<String, String> extraParams)
      throws SessionException {
    this(session, DEFAULT_IP, port, type, DEFAULT_SCHEMA_PREFIX, DEFAULT_PREFIX, extraParams);
  }

  public TempDummyDataSource(
      Session session,
      String ip,
      int port,
      StorageEngineType type,
      String schemaPrefix,
      String dataPrefix,
      Map<String, String> extraParams)
      throws SessionException {
    this.session = Objects.requireNonNull(session);
    this.ip = Objects.requireNonNull(ip);
    this.port = port;
    this.type = Objects.requireNonNull(type);
    this.schemaPrefix = Objects.requireNonNull(schemaPrefix);
    this.dataPrefix = Objects.requireNonNull(dataPrefix);
    this.extraParams = Objects.requireNonNull(extraParams);
    init();
  }

  private void init() throws SessionException {
    LinkedHashMap<String, String> params = new LinkedHashMap<>(extraParams);
    if (params.containsKey("schema_prefix")) {
      throw new IllegalArgumentException("schema_prefix is a reserved key");
    }
    if (params.containsKey("data_prefix")) {
      throw new IllegalArgumentException("data_prefix is a reserved key");
    }
    if (params.containsKey("has_data")) {
      throw new IllegalArgumentException("has_data is a reserved key");
    }
    if (params.containsKey("is_read_only")) {
      throw new IllegalArgumentException("is_read_only is a reserved key");
    }
    if (!schemaPrefix.isEmpty()) {
      params.put("schema_prefix", schemaPrefix);
    }
    if (!dataPrefix.isEmpty()) {
      params.put("data_prefix", dataPrefix);
    }
    params.put("has_data", "true");
    params.put("is_read_only", "true");
    session.addStorageEngine(ip, port, type, params);
  }

  @Override
  public void close() throws SessionException {
    RemovedStorageEngineInfo info =
        new RemovedStorageEngineInfo(ip, port, schemaPrefix, dataPrefix);
    session.removeStorageEngine(Collections.singletonList(info), true);
  }
}
