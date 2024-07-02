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
package cn.edu.tsinghua.iginx.jdbc;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IginXDriver implements Driver {

  private static final boolean IGINX_JDBC_COMPLIANT = false;

  static {
    try {
      DriverManager.registerDriver(new IginXDriver());
    } catch (SQLException e) {
      log.error("Error occurs when registering IginX driver", e);
    }
  }

  private final String IGINX_URL_PREFIX = Config.IGINX_URL_PREFIX + ".*";

  public IginXDriver() {}

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    try {
      return acceptsURL(url) ? new IginXConnection(url, info) : null;
    } catch (Exception e) {
      throw new SQLException(
          "Connection Error, please check whether the network is available or the server has started.");
    }
  }

  @Override
  public boolean acceptsURL(String url) {
    return Pattern.matches(IGINX_URL_PREFIX, url);
  }

  @Override
  public int getMajorVersion() {
    return 0;
  }

  @Override
  public int getMinorVersion() {
    return 0;
  }

  @Override
  public boolean jdbcCompliant() {
    return IGINX_JDBC_COMPLIANT;
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    throw new SQLFeatureNotSupportedException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException(Constant.METHOD_NOT_SUPPORTED);
  }
}
