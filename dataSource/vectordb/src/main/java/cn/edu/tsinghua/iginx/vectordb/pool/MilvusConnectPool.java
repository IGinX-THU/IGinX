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
package cn.edu.tsinghua.iginx.vectordb.pool;

import io.milvus.v2.client.MilvusClientV2;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class MilvusConnectPool extends GenericObjectPool<MilvusClientV2> {

  public MilvusConnectPool(PooledObjectFactory<MilvusClientV2> factory) {
    super(factory);
  }

  /**
   * Creates a new {@code GenericObjectPool} using a specific configuration.
   *
   * @param factory The object factory to be used to create object instances used by this pool
   * @param config The configuration to use for this pool instance. The configuration is used by
   *     value. Subsequent changes to the configuration object will not be reflected in the pool.
   */
  public MilvusConnectPool(
      PooledObjectFactory<MilvusClientV2> factory, GenericObjectPoolConfig<MilvusClientV2> config) {
    super(factory, config);
  }

  /**
   * Creates a new {@code GenericObjectPool} that tracks and destroys objects that are checked out,
   * but never returned to the pool.
   *
   * @param factory The object factory to be used to create object instances used by this pool
   * @param config The base pool configuration to use for this pool instance. The configuration is
   *     used by value. Subsequent changes to the configuration object will not be reflected in the
   *     pool.
   * @param abandonedConfig Configuration for abandoned object identification and removal. The
   *     configuration is used by value.
   */
  public MilvusConnectPool(
      PooledObjectFactory<MilvusClientV2> factory,
      GenericObjectPoolConfig<MilvusClientV2> config,
      AbandonedConfig abandonedConfig) {
    super(factory, config, abandonedConfig);
  }

  /** 获取MilvusClient实例 */
  public MilvusClientV2 getMilvusClient() throws Exception {
    return super.borrowObject();
  }

  /** 归还MilvusClient实例 */
  public void releaseMilvusClient(MilvusClientV2 milvusClient) {
    if (milvusClient != null) {
      super.returnObject(milvusClient);
    }
  }
}
