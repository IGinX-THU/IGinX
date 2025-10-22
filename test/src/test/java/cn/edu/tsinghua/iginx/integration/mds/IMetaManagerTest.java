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

import static org.junit.jupiter.api.Assertions.*;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class IMetaManagerTest {

  private static IMetaManager iMetaManager;

  @BeforeAll
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

  @AfterAll
  public static void afterClass() {
    iMetaManager = null;
  }

  @AfterEach
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

  @ParameterizedTest
  @MethodSource("provideColumnsIntervals")
  public void specialCharInColumnIntervalOfFragmentMeta(ColumnsInterval interval) throws Exception {
    FragmentMeta fragmentMeta =
        new FragmentMeta(interval, KeyInterval.getDefaultKeyInterval(), "test");
    iMetaManager.addFragment(fragmentMeta);
    try {
      long pointsCount = System.identityHashCode(fragmentMeta);
      iMetaManager.updateFragmentPoints(fragmentMeta, pointsCount);
      Map<FragmentMeta, Long> points = iMetaManager.loadFragmentPoints();
      assertEquals(pointsCount, points.get(fragmentMeta).longValue());
    } finally {
      iMetaManager.removeFragment(fragmentMeta);
    }
  }

  private static Stream<ColumnsInterval> provideColumnsIntervals() {
    return Stream.concat(
        Stream.of(
            new ColumnsInterval("start column", "end column"), // 正常情况
            new ColumnsInterval(null, null), // 两端为 null
            new ColumnsInterval("null", "null"), // 两端为 null
            new ColumnsInterval("start column", null), // 仅结束列为 null
            new ColumnsInterval(null, "end column"), // 仅开始列为 null
            new ColumnsInterval("", ""), // 空字符串
            new ColumnsInterval(" ", " "), // 空格
            new ColumnsInterval(
                "data.sensor\\.1.temp",
                "data.sensor\\.2.temp"), // Basic directory structure with escaped dots
            new ColumnsInterval(
                "sys.dev\\.a.metrics", "sys.dev\\.z.metrics"), // Mixed escaped and directory dot
            new ColumnsInterval(
                "log\\.2024\\.03.data",
                "log\\.2024\\.04.data"), // Multiple escaped dots in sequence
            new ColumnsInterval(
                "org.apache.commons\\.io.files",
                "org.apache.commons\\.lang.utils"), // Deeper directory structure
            new ColumnsInterval(
                "com.app\\.v1\\.0.config",
                "com.app\\.v2\\.0.config"), // Complex path with version numbers
            new ColumnsInterval("特殊字符.测试", "结束列.测试"), // Unicode 字符
            new ColumnsInterval("\\Dab.cd\\N-", "-ab\\\\.c\\.d") // 包含转义字符
            ),
        "-\\_:@#$~^{}\""
            .chars()
            .mapToObj(c -> new ColumnsInterval("start" + (char) c, "end" + (char) c)));
  }
}
