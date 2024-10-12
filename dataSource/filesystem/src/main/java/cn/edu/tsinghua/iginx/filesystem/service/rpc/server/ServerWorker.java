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
package cn.edu.tsinghua.iginx.filesystem.service.rpc.server;

import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.filesystem.common.FileSystemException;
import cn.edu.tsinghua.iginx.filesystem.service.Service;
import cn.edu.tsinghua.iginx.filesystem.struct.DataTarget;
import cn.edu.tsinghua.iginx.filesystem.thrift.*;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerWorker implements FileSystemRpc.Iface {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerWorker.class);

  private final Service service;

  public ServerWorker(Service service) {
    this.service = service;
  }

  @Override
  public Map<DataUnit, DataBoundary> getUnits(RawPrefix prefix) throws RpcException {
    String iginxPrefix = ServerObjectMappingUtils.resolveRawPrefix(prefix);
    try {
      Map<DataUnit, DataBoundary> units = service.getUnits(iginxPrefix);
      Map<DataUnit, DataBoundary> result = new HashMap<>();
      for (Map.Entry<DataUnit, DataBoundary> entry : units.entrySet()) {
        DataUnit dataUnit = entry.getKey();
        DataBoundary dataBoundary = entry.getValue();
        result.put(dataUnit, dataBoundary);
      }
      return result;
    } catch (FileSystemException e) {
      LOGGER.error("failed to getUnits({})", prefix, e);
      throw new RpcException(Status.FileSystemException, e.getMessage());
    }
  }

  @Override
  public RawDataSet query(DataUnit unit, RawDataTarget target, RawAggregate aggregate)
      throws RpcException {
    DataTarget dataTarget = ServerObjectMappingUtils.resolveRawDataTarget(target);
    AggregateType aggregateType = ServerObjectMappingUtils.resolveRawAggregate(aggregate);
    try {
      RowStream rowStream = service.query(unit, dataTarget, aggregateType);
      return ServerObjectMappingUtils.constructRawDataSet(rowStream);
    } catch (FileSystemException e) {
      LOGGER.error("failed to query({}, {}, {})", unit, target, aggregate, e);
      throw new RpcException(Status.FileSystemException, e.getMessage());
    }
  }

  @Override
  public void delete(DataUnit unit, RawDataTarget target) throws RpcException {
    DataTarget dataTarget = ServerObjectMappingUtils.resolveRawDataTarget(target);
    try {
      service.delete(unit, dataTarget);
    } catch (FileSystemException e) {
      LOGGER.error("failed to delete({}, {})", unit, target, e);
      throw new RpcException(Status.FileSystemException, e.getMessage());
    }
  }

  @Override
  public void insert(DataUnit unit, RawInserted data) throws RpcException {
    DataView dataView = ServerObjectMappingUtils.resolveRawInserted(data);
    try {
      service.insert(unit, dataView);
    } catch (FileSystemException e) {
      LOGGER.error("failed to insert({}, {})", unit, data, e);
      throw new RpcException(Status.FileSystemException, e.getMessage());
    }
  }
}
