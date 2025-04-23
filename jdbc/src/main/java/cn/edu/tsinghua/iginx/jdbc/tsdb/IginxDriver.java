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
package cn.edu.tsinghua.iginx.jdbc.tsdb;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class IginxDriver implements Driver {

  private static final Logger LOGGER = Logger.getLogger(IginxDriver.class.getName());

  public static final String IGINX_URL_PREFIX = "jdbc:iginx:tsdb:";

  static {
    try {
      DriverManager.registerDriver(new IginxDriver(0, 6));
    } catch (SQLException e) {
      LOGGER.log(Level.SEVERE, "Failed to register IginxDriver", e);
    }
  }

  private final int majorVersion;
  private final int minorVersion;

  public IginxDriver() {
    this(0, 6);
  }

  private IginxDriver(int majorVersion, int minorVersion) {
    this.majorVersion = majorVersion;
    this.minorVersion = minorVersion;
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    return acceptsURL(url) ? new IginxConnection(url, info): null;
  }

  @Override
  public boolean acceptsURL(String url) {
    return url.startsWith(IGINX_URL_PREFIX);
  }

  @Override
  public int getMajorVersion() {
    return majorVersion;
  }

  @Override
  public int getMinorVersion() {
    return minorVersion;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    throw new SQLFeatureNotSupportedException("getPropertyInfo");
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException("getParentLogger");
  }
}
