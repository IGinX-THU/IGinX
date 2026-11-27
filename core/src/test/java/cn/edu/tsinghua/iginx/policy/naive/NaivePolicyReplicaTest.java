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
package cn.edu.tsinghua.iginx.policy.naive;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.conf.*;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.lang.reflect.Proxy;
import java.util.*;
import org.junit.Test;

public class NaivePolicyReplicaTest {
  @Test
  public void notEnoughEngines() {
    Config config = ConfigDescriptor.getInstance().getConfig();
    int oldReplica = config.getReplicaNum(), oldInstances = config.getInstancesNumPerClient();
    String oldClients = config.getClients();
    try {
      config.setInstancesNumPerClient(1);
      for (int replicas : new int[] {0, 1})
        for (int engines : new int[] {1, 2}) {
          config.setReplicaNum(replicas);
          List<StorageEngineMeta> metas = new ArrayList<>();
          for (int i = 0; i < engines; i++)
            metas.add(
                new StorageEngineMeta(
                    i,
                    "127.0.0.1",
                    9000 + i,
                    Collections.emptyMap(),
                    StorageEngineType.filesystem,
                    0));
          IMetaManager meta =
              (IMetaManager)
                  Proxy.newProxyInstance(
                      IMetaManager.class.getClassLoader(),
                      new Class[] {IMetaManager.class},
                      (proxy, method, args) -> {
                        if (method.getName().equals("getWritableStorageEngineList")) return metas;
                        if (method.getName().equals("getStorageEngineNum")) return metas.size();
                        if (method.getName().equals("getIginxId")) return 0L;
                        return null;
                      });
          NaivePolicy policy = new NaivePolicy();
          policy.init(meta);
          InsertStatement insert =
              new InsertStatement(cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType.Row);
          insert.setPaths(Arrays.asList("a.x", "b.y"));
          insert.setKeys(Arrays.asList(1L, 2L));
          policy.notify(insert);
          for (boolean initial : new boolean[] {false, true}) {
            config.setClients(initial ? "a,b" : "");
            try {
              Pair<List<FragmentMeta>, List<StorageUnitMeta>> result =
                  initial
                      ? policy.generateInitialFragmentsAndStorageUnits(insert)
                      : policy.generateFragmentsAndStorageUnits(insert);
              assertTrue(engines >= replicas + 1);
              for (StorageUnitMeta unit : result.getV())
                assertEquals(replicas, unit.getReplicas().size());
            } catch (IllegalArgumentException e) {
              assertTrue(engines < replicas + 1);
              assertEquals("not enough storage engines for replication", e.getMessage());
            }
          }
        }
    } finally {
      config.setReplicaNum(oldReplica);
      config.setClients(oldClients);
      config.setInstancesNumPerClient(oldInstances);
    }
  }
}
