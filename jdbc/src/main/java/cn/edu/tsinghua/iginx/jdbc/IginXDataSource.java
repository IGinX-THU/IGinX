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

import cn.edu.tsinghua.iginx.exception.SessionException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;

public class IginXDataSource implements DataSource {

  private static final Logger LOGGER = Logger.getLogger(IginXDataSource.class.getName());
  private String url;
  private String user;
  private String password;
  private Properties properties;
  private Integer port = 6888;

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
    properties.setProperty(Config.USER, user);
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
    properties.setProperty(Config.PASSWORD, user);
  }

  public Properties getProperties() {
    return properties;
  }

  public void setProperties(Properties properties) {
    this.properties = properties;
  }

  public Integer getPort() {
    return port;
  }

  public void setPort(Integer port) {
    this.port = port;
  }

  @Override
  public Connection getConnection() throws SQLException {
    try {
      return new IginXConnection(url, properties);
    } catch (SessionException e) {
      LOGGER.log(Level.SEVERE, "unexpected error: ", e);
    }
    return null;
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    try {
      Properties newProp = new Properties();
      newProp.setProperty(Config.USER, username);
      newProp.setProperty(Config.PASSWORD, password);
      return new IginXConnection(url, newProp);
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, "unexpected error: ", e);
    }
    return null;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Logger getParentLogger() {
    return null;
  }
}
