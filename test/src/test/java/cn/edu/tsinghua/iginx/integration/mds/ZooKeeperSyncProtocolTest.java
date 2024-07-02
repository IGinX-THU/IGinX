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
package cn.edu.tsinghua.iginx.integration.mds;

import cn.edu.tsinghua.iginx.metadata.sync.protocol.NetworkException;
import cn.edu.tsinghua.iginx.metadata.sync.protocol.SyncProtocol;
import cn.edu.tsinghua.iginx.metadata.sync.protocol.zk.ZooKeeperSyncProtocolImpl;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperSyncProtocolTest extends SyncProtocolTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperSyncProtocolTest.class);

  public static final String CONNECT_STRING = "127.0.0.1:2181";

  @Override
  protected SyncProtocol newSyncProtocol(String category) {
    CuratorFramework client =
        CuratorFrameworkFactory.builder()
            .connectString(CONNECT_STRING)
            .connectionTimeoutMs(15000)
            .retryPolicy(new RetryForever(1000))
            .build();
    client.start();
    try {
      return new ZooKeeperSyncProtocolImpl(category, client, null);
    } catch (NetworkException e) {
      LOGGER.error("[newSyncProtocol] create sync protocol failure: ", e);
    }
    return null;
  }
}
