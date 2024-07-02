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
package cn.edu.tsinghua.iginx.metadata.entity;

import cn.edu.tsinghua.iginx.pool.IginxInfo;
import java.util.Map;

public final class IginxMeta {

  /** iginx 的 id */
  private final long id;

  /** iginx 所在 ip */
  private final String ip;

  /** iginx 对外暴露的端口 */
  private final int port;

  /** iginx 其他控制参数 */
  private final Map<String, String> extraParams;

  public IginxMeta(long id, String ip, int port, Map<String, String> extraParams) {
    this.id = id;
    this.ip = ip;
    this.port = port;
    this.extraParams = extraParams;
  }

  public long getId() {
    return id;
  }

  public String getIp() {
    return ip;
  }

  public int getPort() {
    return port;
  }

  public Map<String, String> getExtraParams() {
    return extraParams;
  }

  public IginxInfo iginxMetaInfo() {
    return new IginxInfo.Builder()
        .host(ip)
        .port(port)
        .user(extraParams.getOrDefault("user", ""))
        .password(extraParams.getOrDefault("password", ""))
        .build();
  }
}
