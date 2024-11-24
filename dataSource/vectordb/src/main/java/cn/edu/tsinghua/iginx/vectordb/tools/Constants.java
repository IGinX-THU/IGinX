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
package cn.edu.tsinghua.iginx.vectordb.tools;

import io.milvus.v2.common.IndexParam;

public class Constants {
  /** 池配置参数：每个键的最大空闲连接数。 */
  public static final String MAX_IDLE_PER_KEY = "maxIdlePerKey";

  /** 池配置参数：每个键的最大总连接数。 */
  public static final String MAX_TOTAL_PER_KEY = "maxTotalPerKey";

  /** 池配置参数：池中所有键的最大总连接数。 */
  public static final String MAX_TOTAL = "maxTotal";

  public static final String MAX_IDLE = "maxIdle";
  public static final String MIN_IDLE = "minIdle";

  /** 池配置参数：获取连接的最大等待时间。 */
  public static final String MAX_BLOCK_WAIT_DURATION = "maxBlockWaitDuration";

  /** 池配置参数：最小可驱逐的空闲时间。 */
  public static final String MIN_EVICTABLE_IDLE_DURATION = "minEvictableIdleDuration";

  /** 默认池配置参数：每个键的最大空闲连接数。 */
  public static final int DEFAULT_MAX_IDLE_PER_KEY = 10;

  /** 默认池配置参数：每个键的最大总连接数。 */
  public static final int DEFAULT_MAX_TOTAL_PER_KEY = 20;

  /** 默认池配置参数：池中所有键的最大总连接数。 */
  public static final int DEFAULT_MAX_TOTAL = 100;

  /** 默认池配置参数：获取连接的最大等待时间（秒）。 */
  public static final long DEFAULT_MAX_BLOCK_WAIT_DURATION = 5L;

  /** 默认池配置参数：最小可驱逐的空闲时间（秒）。 */
  public static final long DEFAULT_MIN_EVICTABLE_IDLE_DURATION = 10L;

  public static final int DEFAULT_MAX_IDLE = 10;

  public static final int DEFAULT_MIN_IDLE = 5;

  /** 用户名参数的键。 */
  public static final String USERNAME = "username";

  /** 密码参数的键。 */
  public static final String PASSWORD = "password";

  /** 数据库名称参数的键。 */
  public static final String DB_NAME = "dbName";

  /** 数据库连接 URI 参数的键。 */
  public static final String DB_URI = "uri";

  /** 数据库连接协议的键。 用于指定连接字符串中的协议类型，例如 "http" 或 "grpc"。 */
  public static final String DB_PROTOCOL = "protocol";

  /** 默认的数据库连接协议。 如果未指定协议，则使用此默认值，通常为 "grpc"。 */
  public static final String DEFAULT_DB_PROTOCOL = "grpc";

  public static final String KEY_NAME = "id";

  public static final String VECTOR_NAME = "vector";

  public static final String TAGKV_SEPARATOR = "-";

  public static final String TAGKV_EQUAL = "=";

  public static final String STAR = "*";

  public static final int DEFAULT_DIMENSION = 1;

  public static final IndexParam.IndexType DEFAULT_INDEX_TYPE = IndexParam.IndexType.FLAT;

  public static final IndexParam.MetricType DEFAULT_METRIC_TYPE = IndexParam.MetricType.L2;

  /** 非dummy数据库默认数据库前缀。 用于在 Milvus 中创建数据库的名称。 */
  public static final String DATABASE_PREFIX = "unit";

  public static final Character QUOTA = '`';

  public static final String PATH_SEPARATOR = ".";

  /** PathSystem中虚拟路径Key */
  public static final String PATH_NAME = "$$PATH$$";

  public static final String PATH_IS_DUMMY = "$$ISDUMMY$$";

  /** Milvus collection properties 里描述字段的前缀 */
  public static final String DYNAMIC_FIELDS_PROPERTIES_PREFIX = "df_";

  /** Milvus collection properties 里描述字段是否被删除的属性名 */
  public static final String KEY_PROPERTY_DELETED = "deleted";

  /** Milvus collection properties 里描述字段的当前名称的属性名 */
  public static final String KEY_PROPERTY_CURRENT_FIELD_NAME = "name";

  public static final String KEY_PROPERTY_FIELD_DATA_TYPE = "type";

  public static final String MILVUS_PRIMARY_FIELD_NAME = "VECTORDBID";

  public static final String MILVUS_VECTOR_FIELD_NAME = "VECTORDBVECTOR";

  public static final int MILVUS_INDEX_PARAM_NLIST = 8;

  public static final String MILVUS_DYNAMIC_FIELD_NAME = "$meta";

  public static final String MILVUS_VECTOR_INDEX_NAME = "VECTORDBINDEX";

  public static final Long MILVUS_QUERY_LIMIT = 16384L;
}
