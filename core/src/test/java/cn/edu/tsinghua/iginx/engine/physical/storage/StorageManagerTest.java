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
package cn.edu.tsinghua.iginx.engine.physical.storage;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class StorageManagerTest {
  private static final long ENGINE_ID = Long.MAX_VALUE;
  private Map<Long, Pair<IStorage, ThreadPoolExecutor>> storageMap;
  private StorageEngineMeta meta;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    Field field = StorageManager.class.getDeclaredField("storageMap");
    field.setAccessible(true);
    storageMap = (Map<Long, Pair<IStorage, ThreadPoolExecutor>>) field.get(null);
    assertFalse(storageMap.containsKey(ENGINE_ID));
    meta =
        new StorageEngineMeta(
            ENGINE_ID, "127.0.0.1", 2881, Collections.emptyMap(), StorageEngineType.relational, 0);
  }

  @After
  public void tearDown() {
    storageMap.remove(ENGINE_ID);
  }

  @Test
  public void missingLocalConnectionIsDisconnected() {
    assertFalse(StorageManager.testEngineConnection(meta));
  }

  @Test
  public void connectionResultIsPreserved() {
    AtomicBoolean connected = new AtomicBoolean(true);
    storageMap.put(ENGINE_ID, new Pair<>(storage(connected), null));
    assertTrue(StorageManager.testEngineConnection(meta));
    connected.set(false);
    assertFalse(StorageManager.testEngineConnection(meta));
  }

  @Test
  public void staleMetadataCanBeCheckedAfterConnectionRemoval() {
    storageMap.put(ENGINE_ID, new Pair<>(storage(new AtomicBoolean(true)), null));
    assertTrue(StorageManager.testEngineConnection(meta));
    // Local release has completed, but a concurrent registration still sees the old metadata.
    storageMap.remove(ENGINE_ID);
    assertFalse(StorageManager.testEngineConnection(meta));
    storageMap.put(ENGINE_ID, new Pair<>(storage(new AtomicBoolean(true)), null));
    assertTrue(StorageManager.testEngineConnection(meta));
  }

  private IStorage storage(AtomicBoolean connected) {
    return (IStorage)
        Proxy.newProxyInstance(
            IStorage.class.getClassLoader(),
            new Class<?>[] {IStorage.class},
            (proxy, method, args) -> {
              if (method.getName().equals("testConnection")) {
                assertSame(meta, args[0]);
                return connected.get();
              }
              throw new AssertionError("Unexpected storage call: " + method.getName());
            });
  }
}
