package cn.edu.tsinghua.iginx.filestore.service.rpc.client.transport;

import org.apache.thrift.transport.TTransport;

public interface TTransportFactory {
  TTransport create() throws Exception;
}
