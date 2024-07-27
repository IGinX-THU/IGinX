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
package cn.edu.tsinghua.iginx.filestore.service.storage;

import cn.edu.tsinghua.iginx.filestore.common.FileStoreException;
import cn.edu.tsinghua.iginx.filestore.service.AbstractServiceTest;
import cn.edu.tsinghua.iginx.filestore.service.Service;
import cn.edu.tsinghua.iginx.filestore.thrift.DataUnit;
import com.typesafe.config.Config;
import org.apache.thrift.transport.TTransportException;

import java.nio.file.Paths;
import java.util.UUID;

public abstract class AbstractStorageServiceTest extends AbstractServiceTest {

  private final StorageConfig config;
  private final DataUnit dataUnit;

  protected AbstractStorageServiceTest(String type, Config config) {
    String root = Paths.get("target", "test", UUID.randomUUID().toString()).toString();
    this.config = new StorageConfig(root, type, config);
    this.dataUnit = new DataUnit(false);
    this.dataUnit.setName("test0001");
  }

  private Service service;

  @Override
  protected Service getService() throws Exception {
    if (service == null) {
      service = new StorageService(config, null);
    }
    return service;
  }

  @Override
  protected DataUnit getUnit() {
    return dataUnit;
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    if (service != null) {
      service.close();
      service = null;
    }
  }

}
