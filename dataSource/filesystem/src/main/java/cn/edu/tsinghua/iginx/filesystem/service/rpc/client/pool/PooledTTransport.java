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
package cn.edu.tsinghua.iginx.filesystem.service.rpc.client.pool;

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
    if (isOpen()) {
      returnToPool();
    }
  }

  public void destroy() {
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
