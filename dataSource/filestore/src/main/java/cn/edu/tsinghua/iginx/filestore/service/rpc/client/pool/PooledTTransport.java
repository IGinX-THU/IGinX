package cn.edu.tsinghua.iginx.filestore.service.rpc.client.pool;

import org.apache.commons.pool2.ObjectPool;
import org.apache.thrift.transport.TTransport;

public class PooledTTransport extends ForwardTTransport {

  final ObjectPool<PooledTTransport> source;

  PooledTTransport(TTransport transport, ObjectPool<PooledTTransport> source) {
    super(transport);
    this.source = source;
  }

  @Override
  public void close() {
    returnToPool();
  }

  void destroy() {
    super.close();
  }

  void returnToPool() {
    try {
      source.returnObject(this);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
