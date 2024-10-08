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
package cn.edu.tsinghua.iginx.filesystem.service.rpc.client.pool;

import cn.edu.tsinghua.iginx.filesystem.service.rpc.client.transport.TTransportFactory;
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
