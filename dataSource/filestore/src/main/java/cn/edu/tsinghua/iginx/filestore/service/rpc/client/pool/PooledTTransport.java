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
