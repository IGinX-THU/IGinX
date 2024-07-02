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

public class Config {

  public static final String IGINX_URL_PREFIX = "jdbc:iginx://";

  public static final String IGINX_DEFAULT_HOST = "localhost";
  public static final int IGINX_DEFAULT_PORT = 6888;

  public static final String USER = "user";
  public static final String DEFAULT_USER = "root";

  public static final String PASSWORD = "password";
  public static final String DEFAULT_PASSWORD = "root";

  public static final int DEFAULT_CONNECTION_TIMEOUT_MS = 0;
}
