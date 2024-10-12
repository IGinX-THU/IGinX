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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.jdbc.tsdb;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.jdbc.tsdb.util.AuthorityInfo;
import cn.edu.tsinghua.iginx.pool.SessionPool;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class IginxConnection implements Connection {

  private static final Logger LOGGER = Logger.getLogger(IginxConnection.class.getName());

  private static final String IGINX_AUTHORITIES_PROPERTY = "iginx.authorities";
  private static final String DEFAULT_IGINX_AUTHORITIES = "";

  private final SessionPool sessionPool;

  private boolean isClosed;

  public IginxConnection(String url, Properties info) throws SQLException {
    String rawAuthorities = info.getProperty(IGINX_AUTHORITIES_PROPERTY, DEFAULT_IGINX_AUTHORITIES);

    List<AuthorityInfo> authorities =
        Arrays.stream(rawAuthorities.split("#"))
            .map(AuthorityInfo::parse)
            .collect(Collectors.toList());
    if (authorities.isEmpty()) {
      throw new SQLException("No authority is provided");
    }

    HashSet<String> localIPSet = new HashSet<>(getLocalIps());
    List<AuthorityInfo> localAuthorities =
        authorities.stream()
            .filter(authorityInfo -> localIPSet.contains(authorityInfo.getHost()))
            .collect(Collectors.toList());

    AuthorityInfo host = randomSelectServer(localAuthorities, authorities);

    this.sessionPool =
        new SessionPool(
            host.getHost(), host.getPort().toString(), host.getUser(), host.getPassword());
    this.isClosed = false;
  }

  private static AuthorityInfo randomSelectServer(
      List<AuthorityInfo> localServers, List<AuthorityInfo> serversInfo) {
    Random random = new Random(0);
    AuthorityInfo server;
    if (localServers.isEmpty()) {
      LOGGER.info("No IGinX server is in the same node with the client, select a random server");
      server = serversInfo.get(random.nextInt(serversInfo.size()));
    } else {
      LOGGER.info("Select a random server from the servers in the same node with the client");
      server = localServers.get(random.nextInt(localServers.size()));
    }
    return server;
  }

  public List<String> getLocalIps() throws SQLException {
    List<String> ips = new ArrayList<>();
    try {
      Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
      InetAddress ip;
      while (networkInterfaces.hasMoreElements()) {
        NetworkInterface networkInterface = networkInterfaces.nextElement();
        Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
        while (addresses.hasMoreElements()) {
          ip = addresses.nextElement();
          if (ip instanceof Inet4Address) {
            ips.add(ip.getHostAddress());
          }
        }
      }
    } catch (SocketException e) {
      throw new SQLException("Failed to get local IPs", e);
    }
    return ips;
  }

  @Override
  public Statement createStatement() throws SQLException {
    return new IginxStatement(this);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    if (sql.equals(IginxPreparedStatement.INSERT_SQL)) {
      return new IginxPreparedStatement(this, sql);
    }
    throw new SQLException("Unsupported sql: " + sql);
  }

  public SessionPool getSessionPool() {
    return sessionPool;
  }

  @Override
  public void close() throws SQLException {
    if (isClosed()) {
      return;
    }

    try {
      sessionPool.close();
    } catch (SessionException e) {
      throw new SQLException("Fail to close Statement", e);
    } finally {
      this.isClosed = true;
    }
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void clearWarnings() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void commit() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isReadOnly() {
    return true;
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getCatalog() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void rollback() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Statement createStatement(
      int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public CallableStatement prepareCall(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Clob createClob() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    throw new SQLClientInfoException();
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    throw new SQLClientInfoException();
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getSchema() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }
}
