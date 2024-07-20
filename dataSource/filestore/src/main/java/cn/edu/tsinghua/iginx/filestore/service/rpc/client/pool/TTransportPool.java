package cn.edu.tsinghua.iginx.filestore.service.rpc.client.pool;

import cn.edu.tsinghua.iginx.filestore.service.rpc.client.transport.TTransportFactory;
import java.util.Objects;
import lombok.Setter;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.thrift.transport.TTransport;

public class TTransportPool extends GenericObjectPool<PooledTTransport> {

  public TTransportPool(
      TTransportFactory factory, GenericObjectPoolConfig<PooledTTransport> config) {
    super(new PooledTTransportFactory(factory), config);
    ((PooledTTransportFactory) getFactory()).setPool(this);
  }

  private static class PooledTTransportFactory implements PooledObjectFactory<PooledTTransport> {

    private final TTransportFactory factory;
    @Setter private TTransportPool pool;

    public PooledTTransportFactory(TTransportFactory factory) {
      this.factory = Objects.requireNonNull(factory);
    }

    @Override
    public void activateObject(PooledObject<PooledTTransport> p) throws Exception {}

    @Override
    public void destroyObject(PooledObject<PooledTTransport> p) throws Exception {
      PooledTTransport transport = p.getObject();
      transport.destroy();
    }

    @Override
    public PooledObject<PooledTTransport> makeObject() throws Exception {
      TTransport transport = factory.create();
      transport.open();
      PooledTTransport pooled = new PooledTTransport(transport, pool);
      return new DefaultPooledObject<>(pooled);
    }

    @Override
    public void passivateObject(PooledObject<PooledTTransport> p) throws Exception {
      PooledTTransport transport = p.getObject();
      if (transport.source != pool) {
        throw new IllegalStateException("Transport not associated with this pool");
      }
    }

    @Override
    public boolean validateObject(PooledObject<PooledTTransport> p) {
      PooledTTransport transport = p.getObject();
      return transport.isOpen();
    }
  }
}
