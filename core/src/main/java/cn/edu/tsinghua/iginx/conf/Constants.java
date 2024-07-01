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
package cn.edu.tsinghua.iginx.conf;

public class Constants {

  public static final int MAX_REDIRECT_TIME = 5;

  public static final String IGINX_HOME = "IGINX_HOME";

  public static final String CONF = "IGINX_CONF";

  public static final String DRIVER = "IGINX_DRIVER";

  public static final String UDF_LIST = "IGINX_UDF_LIST";

  public static final String CONFIG_FILE = "conf/config.properties";

  public static final String UDF_LIST_FILE = "udf_list";

  public static final String DRIVER_DIR = "driver";

  public static final String FILE_META = "file";

  public static final String ZOOKEEPER_META = "zookeeper";

  public static final String ETCD_META = "etcd";

  public static final String LEVEL_SEPARATOR = ".";

  public static final String LEVEL_PLACEHOLDER = "*";

  public static final String HAS_DATA = "has_data";

  public static final String IS_READ_ONLY = "is_read_only";

  public static final String DATA_PREFIX = "data_prefix";

  // especially for embedded storage engines: parquet, filesystem
  public static final String EMBEDDED_PREFIX = "embedded_prefix";

  public static final String SCHEMA_PREFIX = "schema_prefix";

  public static final String DUMMY = "dummy";

  public static final String UDAF = "udaf";

  public static final String UDTF = "udtf";

  public static final String UDSF = "udsf";

  public static final String TRANSFORM = "transform";

  // for ZooKeeper
  // The sequence number is always fixed length of 10 digits, 0 padded.
  public static int LENGTH_OF_SEQUENCE_NUMBER = 10;
}
