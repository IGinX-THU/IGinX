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
package cn.edu.tsinghua.iginx.filesystem.service.storage;

import cn.edu.tsinghua.iginx.filesystem.service.AbstractServiceTest;
import cn.edu.tsinghua.iginx.filesystem.service.Service;
import cn.edu.tsinghua.iginx.filesystem.thrift.DataUnit;
import com.typesafe.config.Config;
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
