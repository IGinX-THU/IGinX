package cn.edu.tsinghua.iginx.filestore.service.rpc.client.transport;

import cn.edu.tsinghua.iginx.filestore.service.rpc.client.ClientConfig;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import java.net.InetSocketAddress;
import java.util.Objects;

public class TSocketFactory implements TTransportFactory {
  private final InetSocketAddress address;
  private final ClientConfig config;

  public TSocketFactory(InetSocketAddress address, ClientConfig config) {
    this.address = Objects.requireNonNull(address);
    this.config = Objects.requireNonNull(config);
  }

  @Override
  public TSocket create() throws TTransportException {
    int socketTimeout = Math.toIntExact(config.getSocketTimeout().toMillis());
    int connectTimeout = Math.toIntExact(config.getConnectTimeout().toMillis());

    return new TSocket(
        new TConfiguration(),
        address.getHostString(),
        address.getPort(),
        socketTimeout,
        connectTimeout);
  }
}
