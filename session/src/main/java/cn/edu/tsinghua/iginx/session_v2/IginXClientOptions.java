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

public final class IginXClientOptions {

  private static final String DEFAULT_USERNAME = "root";

  private static final String DEFAULT_PASSWORD = "root";

  private final String host;

  private final int port;

  private final String username;

  private final String password;

  private IginXClientOptions(IginXClientOptions.Builder builder) {
    this.host = builder.host;
    this.port = builder.port;
    this.username = builder.username;
    this.password = builder.password;
  }

  public static IginXClientOptions.Builder builder() {
    return new IginXClientOptions.Builder();
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public static class Builder {

    private String host;

    private int port;

    private String username;

    private String password;

    private Builder() {}

    public IginXClientOptions.Builder url(String url) {
      Arguments.checkUrl(url, "url");
      // TODO: 将 url 拆分成 host + port
      return this;
    }

    public IginXClientOptions.Builder host(String host) {
      Arguments.checkNonEmpty(host, "host");
      this.host = host;
      return this;
    }

    public IginXClientOptions.Builder port(int port) {
      this.port = port;
      return this;
    }

    public IginXClientOptions.Builder authenticate(String username, String password) {
      Arguments.checkNonEmpty(username, "username");
      Arguments.checkNonEmpty(password, "password");
      this.username = username;
      this.password = password;
      return this;
    }

    public IginXClientOptions.Builder username(String username) {
      Arguments.checkNonEmpty(username, "username");
      this.username = username;
      return this;
    }

    public IginXClientOptions.Builder password(String password) {
      Arguments.checkNonEmpty(password, "password");
      this.password = password;
      return this;
    }

    public IginXClientOptions build() {
      if (this.host == null || this.port == 0) {
        throw new IllegalStateException("the host and port to connect to Iginx has to be defined.");
      }
      if (this.username == null) {
        this.username = DEFAULT_USERNAME;
      }
      if (this.password == null) {
        this.password = DEFAULT_PASSWORD;
      }
      return new IginXClientOptions(this);
    }
  }
}
