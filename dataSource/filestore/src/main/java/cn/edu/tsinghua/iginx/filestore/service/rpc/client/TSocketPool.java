package cn.edu.tsinghua.iginx.filestore.service.rpc.client;

import cn.edu.tsinghua.iginx.filestore.service.rpc.RpcConfig;
import cn.edu.tsinghua.iginx.filestore.service.rpc.client.pool.PooledTTransport;
import cn.edu.tsinghua.iginx.filestore.service.rpc.client.pool.TTransportPool;
import cn.edu.tsinghua.iginx.filestore.service.rpc.client.pool.TTransportPoolConfig;
import cn.edu.tsinghua.iginx.filestore.service.rpc.client.transport.TSocketFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.net.InetSocketAddress;

public class TSocketPool extends TTransportPool {
  public TSocketPool(InetSocketAddress address,ClientConfig config) {
    super(new TSocketFactory(address,config), createGenericPoolConfig(config.getConnectPool()));
  }

  private static GenericObjectPoolConfig<PooledTTransport> createGenericPoolConfig(
      TTransportPoolConfig config) {
    GenericObjectPoolConfig<PooledTTransport> poolConfig = new GenericObjectPoolConfig<>();
    poolConfig.setMaxTotal(config.getMaxTotal());
    poolConfig.setMinEvictableIdleDuration(config.getMinEvictableIdleDuration());
    return poolConfig;
  }
}
