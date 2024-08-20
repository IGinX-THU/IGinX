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

package cn.edu.tsinghua.iginx.filestore.service.rpc.client;

import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.filestore.common.FileStoreException;
import cn.edu.tsinghua.iginx.filestore.service.Service;
import cn.edu.tsinghua.iginx.filestore.service.rpc.client.pool.PooledTTransport;
import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;
import cn.edu.tsinghua.iginx.filestore.thrift.*;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import java.net.InetSocketAddress;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteService implements Service {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteService.class);

  protected final TSocketPool pool;

  public RemoteService(InetSocketAddress address, ClientConfig config) {
    LOGGER.info("will connect to file store server at {}", address);
    this.pool = new TSocketPool(address, config);
  }

  private FileStoreRpc.Client wrapClient(TTransport transport) {
    return new FileStoreRpc.Client(new TBinaryProtocol(transport));
  }

  private void handleRpcException(String action, RpcException e) throws RemoteFileStoreException {
    String msg =
        String.format("failed to %s remotely: %s(%s)", action, e.getStatus(), e.getMessage());
    switch (e.getStatus()) {
      case FileStoreException:
        throw new RemoteFileStoreException(msg, e);
      case OK:
      case UnknownException:
      default:
        throw new IllegalStateException(msg, e);
    }
  }

  @Override
  public Map<DataUnit, DataBoundary> getUnits(@Nullable String prefix) throws FileStoreException {
    RawPrefix rawPrefix = ClientObjectMappingUtils.constructRawPrefix(prefix);
    try (PooledTTransport transport = pool.borrowObject()) {
      FileStoreRpc.Client client = wrapClient(transport);
      try {
        return client.getUnits(rawPrefix);
      } catch (Exception e) {
        transport.destroy();
        throw e;
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (RpcException e) {
      handleRpcException("get units", e);
      throw new IllegalStateException("unreachable", e);
    } catch (Exception e) {
      throw new RemoteFileStoreException("failed to get storage units", e);
    }
  }

  @Override
  public RowStream query(DataUnit unit, DataTarget target, @Nullable AggregateType aggregate)
      throws FileStoreException {
    RawDataTarget rawTarget = ClientObjectMappingUtils.constructRawDataTarget(target);
    RawAggregate rawAggregate = ClientObjectMappingUtils.constructRawAggregate(aggregate);
    try (PooledTTransport transport = pool.borrowObject()) {
      FileStoreRpc.Client client = wrapClient(transport);
      try {
        RawDataSet dataSet = client.query(unit, rawTarget, rawAggregate);
        return ClientObjectMappingUtils.constructRowStream(dataSet);
      } catch (Exception e) {
        transport.destroy();
        throw e;
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (RpcException e) {
      handleRpcException("query", e);
      throw new IllegalStateException("unreachable", e);
    } catch (Exception e) {
      throw new IllegalStateException("failed to query", e);
    }
  }

  @Override
  public void delete(DataUnit dataUnit, DataTarget target) throws FileStoreException {
    RawDataTarget rawTarget = ClientObjectMappingUtils.constructRawDataTarget(target);
    try (PooledTTransport transport = pool.borrowObject()) {
      FileStoreRpc.Client client = wrapClient(transport);
      try {
        client.delete(dataUnit, rawTarget);
      } catch (Exception e) {
        transport.destroy();
        throw e;
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (RpcException e) {
      handleRpcException("delete", e);
      throw new IllegalStateException("unreachable", e);
    } catch (Exception e) {
      throw new RemoteFileStoreException("failed to delete", e);
    }
  }

  @Override
  public void insert(DataUnit unit, DataView dataView) throws FileStoreException {
    RawInserted rawInserted = ClientObjectMappingUtils.constructRawInserted(dataView);
    try (PooledTTransport transport = pool.borrowObject()) {
      FileStoreRpc.Client client = wrapClient(transport);
      try {
        client.insert(unit, rawInserted);
      } catch (Exception e) {
        transport.destroy();
        throw e;
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (RpcException e) {
      handleRpcException("insert", e);
      throw new IllegalStateException("unreachable", e);
    } catch (Exception e) {
      throw new RemoteFileStoreException("failed to insert", e);
    }
  }

  @Override
  public void close() {
    pool.close();
  }
}
