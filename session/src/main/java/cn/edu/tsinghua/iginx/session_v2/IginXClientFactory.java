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
package cn.edu.tsinghua.iginx.session_v2;

import cn.edu.tsinghua.iginx.session_v2.internal.IginXClientImpl;

public final class IginXClientFactory {

  private IginXClientFactory() {}

  public static IginXClient create() {
    return create("127.0.0.1", 6888);
  }

  public static IginXClient create(String url) {
    IginXClientOptions options = IginXClientOptions.builder().url(url).build();
    return create(options);
  }

  public static IginXClient create(String host, int port) {
    IginXClientOptions options = IginXClientOptions.builder().host(host).port(port).build();
    return create(options);
  }

  public static IginXClient create(String url, String username, String password) {
    IginXClientOptions options =
        IginXClientOptions.builder().url(url).username(username).password(password).build();
    return create(options);
  }

  public static IginXClient create(String host, int port, String username, String password) {
    IginXClientOptions options =
        IginXClientOptions.builder()
            .host(host)
            .port(port)
            .username(username)
            .password(password)
            .build();
    return create(options);
  }

  public static IginXClient create(IginXClientOptions options) {
    Arguments.checkNotNull(options, "IginXClientOptions");
    return new IginXClientImpl(options);
  }
}
