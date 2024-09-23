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
package cn.edu.tsinghua.iginx.filesystem.service;

import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.filesystem.common.FileSystemException;
import cn.edu.tsinghua.iginx.filesystem.service.rpc.client.RemoteService;
import cn.edu.tsinghua.iginx.filesystem.service.rpc.server.Server;
import cn.edu.tsinghua.iginx.filesystem.service.storage.StorageService;
import cn.edu.tsinghua.iginx.filesystem.struct.DataTarget;
import cn.edu.tsinghua.iginx.filesystem.thrift.DataBoundary;
import cn.edu.tsinghua.iginx.filesystem.thrift.DataUnit;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import java.net.InetSocketAddress;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemService implements Service {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemService.class);

  private final Service service;
  private final Server server;

  public FileSystemService(InetSocketAddress address, FileSystemConfig config)
      throws FileSystemException {
    if (config.isServe()) {
      this.service = new StorageService(config.getData(), config.getDummy());
      Server temp = null;
      try {
        temp = new Server(address, this.service);
      } catch (TTransportException | InterruptedException e) {
        // TODO: 无法创建服务端难道不应该抛出异常吗？来让客户端知道添加失败。
        //       过去的代码中也有类似的问题，但是没有处理。而且测试的运行依赖这个BUG
        LOGGER.error("failed to start thrift server at {}", address, e);
      }
      this.server = temp;
    } else {
      this.service = new RemoteService(address, config.getClient());
      this.server = null;
    }
  }

  @Override
  public Map<DataUnit, DataBoundary> getUnits(@Nullable String prefix) throws FileSystemException {
    return service.getUnits(prefix);
  }

  @Override
  public RowStream query(DataUnit unit, DataTarget target, @Nullable AggregateType aggregate)
      throws FileSystemException {
    return service.query(unit, target, aggregate);
  }

  @Override
  public void delete(DataUnit unit, DataTarget target) throws FileSystemException {
    service.delete(unit, target);
  }

  @Override
  public void insert(DataUnit unit, DataView dataView) throws FileSystemException {
    service.insert(unit, dataView);
  }

  @Override
  public void close() throws FileSystemException {
    if (server != null) {
      server.close();
    }
    service.close();
  }
}
