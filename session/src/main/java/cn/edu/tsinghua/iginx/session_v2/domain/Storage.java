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
package cn.edu.tsinghua.iginx.session_v2.domain;

import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import java.util.Map;

public final class Storage {

  private final String ip;

  private final int port;

  private final StorageEngineType type;

  private final Map<String, String> extraParams;

  public Storage(String ip, int port, StorageEngineType type, Map<String, String> extraParams) {
    this.ip = ip;
    this.port = port;
    this.type = type;
    this.extraParams = extraParams;
  }

  public String getIp() {
    return ip;
  }

  public int getPort() {
    return port;
  }

  public StorageEngineType getType() {
    return type;
  }

  public Map<String, String> getExtraParams() {
    return extraParams;
  }
}
