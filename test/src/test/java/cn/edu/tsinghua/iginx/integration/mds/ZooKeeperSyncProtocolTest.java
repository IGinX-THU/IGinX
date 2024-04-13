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
