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
package cn.edu.tsinghua.iginx.integration.mds;

import static org.junit.Assert.assertEquals;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.*;

public class IMetaManagerTest {

  private static IMetaManager iMetaManager;

  @BeforeClass
  public static void beforeClass() {
    if (System.getenv("STORAGE") != null) {
      switch (System.getenv("STORAGE")) {
        case "zookeeper":
          ConfigDescriptor.getInstance().getConfig().setMetaStorage("zookeeper");
          ConfigDescriptor.getInstance()
              .getConfig()
              .setZookeeperConnectionString(System.getenv("ZOOKEEPER_CONNECTION_STRING"));
          System.out.println("use zookeeper as meta storage engine");
          break;
        case "etcd":
          ConfigDescriptor.getInstance().getConfig().setMetaStorage("etcd");
          ConfigDescriptor.getInstance()
              .getConfig()
              .setEtcdEndpoints(System.getenv("ETCD_ENDPOINTS"));
          System.out.println("use etcd as meta storage engine");
          break;
      }
    }
    ConfigDescriptor.getInstance().getConfig().setStorageEngineList("");
    iMetaManager = DefaultMetaManager.getInstance();
  }

  @AfterClass
  public static void afterClass() {
    iMetaManager = null;
  }

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  @Test
  public void storageEngineTest() {
    List<StorageEngineMeta> storageEngines = iMetaManager.getStorageEngineList();
    // 初始情况下没有存储数据后端
    assertEquals(0, storageEngines.size());
    // 增加一个数据后端
    Map<String, String> extraParams = new HashMap<>();
    extraParams.put("username", "root");
    extraParams.put("password", "root");
    extraParams.put("sessionPoolSize", "20");
    StorageEngineMeta engine1 =
        new StorageEngineMeta(
            0,
            "127.0.0.1",
            1001,
            extraParams,
            StorageEngineType.iotdb12,
            iMetaManager.getIginxId());
    iMetaManager.addStorageEngines(Collections.singletonList(engine1));
    // 查询数据后端
    storageEngines = iMetaManager.getStorageEngineList();
    assertEquals(1, storageEngines.size());
    assertEquals(1001, storageEngines.get(0).getPort());
    assertEquals(iMetaManager.getIginxId(), storageEngines.get(0).getCreatedBy());
  }

  @Test
  public void storageUnitAndFragmentTest() {
    // TODO: 测试优先级
  }
}
