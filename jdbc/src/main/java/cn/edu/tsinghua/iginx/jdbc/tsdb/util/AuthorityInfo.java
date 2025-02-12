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
package cn.edu.tsinghua.iginx.jdbc.tsdb.util;

import java.net.URI;

public class AuthorityInfo {
  private final String user;
  private final String password;
  private final String host;
  private final Integer port;

  public AuthorityInfo(String user, String password, String host, Integer port) {
    this.user = user;
    this.password = password;
    this.host = host;
    this.port = port;
  }

  public static AuthorityInfo parse(String authority) {
    String temp = "temp:";
    if (authority != null && !authority.isEmpty()) {
      temp += "//" + authority;
    }
    temp += "/";
    URI uri = URI.create(temp);
    String userInfo = uri.getUserInfo();
    String user = null;
    String password = null;
    if (userInfo != null) {
      String[] userAndPassword = userInfo.split(":");
      user = userAndPassword[0];
      if (userAndPassword.length > 1) {
        password = userAndPassword[1];
      }
    }
    String host = uri.getHost();
    Integer port = uri.getPort();
    if (port == -1) {
      port = null;
    }
    return new AuthorityInfo(user, password, host, port);
  }

  public String getUser() {
    return user == null ? "root" : user;
  }

  public String getPassword() {
    return password == null ? "root" : password;
  }

  public String getHost() {
    return host == null ? "127.0.0.1" : host;
  }

  public Integer getPort() {
    return port == null ? 6888 : port;
  }
}
