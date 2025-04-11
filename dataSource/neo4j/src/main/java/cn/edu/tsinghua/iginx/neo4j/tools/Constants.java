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
package cn.edu.tsinghua.iginx.neo4j.tools;

public class Constants {
  /** 池配置参数：最大连接数。 */
  public static final String MAX_CONNECTION_POOL_SIZE = "poolSize";

  /** 池配置参数：连接超时时间。 */
  public static final String CONNECTION_TIMEOUT = "timeout";

  public static final String CONNECTION_CHECK_TIMEOUT = "checkTimeout";

  public static final int DEFAULT_MAX_CONNECTION_POOL_SIZE = 50;

  public static final int DEFAULT_CONNECTION_TIMEOUT = 100000;

  public static final int DEFAULT_CONNECTION_CHECK_TIMEOUT = 300;

  public static final String IDENTITY_PROPERTY_NAME = "NEO4JID";

  public static final String TAGKV_SEPARATOR = "-";

  public static final String TAGKV_EQUAL = "=";

  public static final String SEPARATOR = ".";

  public static final String DATABASE_PREFIX = "unit";
}
