/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.engine.physical.storage;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageManager.class);

  private static final Map<StorageEngineType, ClassLoader> classLoaders = new ConcurrentHashMap<>();

  private static boolean hasInitLoaders = false;

  private static final Map<StorageEngineType, String> drivers = new ConcurrentHashMap<>();

  private static final Map<Long, Pair<IStorage, ThreadPoolExecutor>> storageMap =
      new ConcurrentHashMap<>();

  public StorageManager(List<StorageEngineMeta> metaList) {
    initClassLoaderAndDrivers();
    for (StorageEngineMeta meta : metaList) {
      if (!initStorage(meta)) {
        System.exit(-1);
      }
    }
  }

  public static Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(StorageEngineMeta meta) {
    return getBoundaryOfStorage(meta, null);
  }

  public static Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(
      StorageEngineMeta meta, String dataPrefix) {
    initClassLoaderAndDrivers();
    StorageEngineType engine = meta.getStorageEngine();
    String driver = drivers.get(engine);
    long id = meta.getId();
    boolean needRelease = false;
    IStorage storage = null;
    try {
      if (storageMap.containsKey(id)) {
        storage = storageMap.get(id).k;
      } else {
        ClassLoader loader = classLoaders.get(engine);
        storage =
            (IStorage)
                loader.loadClass(driver).getConstructor(StorageEngineMeta.class).newInstance(meta);
        if (!engine.equals(StorageEngineType.filesystem)) {
          needRelease = true;
        }
      }
      return storage.getBoundaryOfStorage(dataPrefix);
    } catch (ClassNotFoundException e) {
      LOGGER.error("load class {} for engine {} failure: {}", driver, engine, e);
    } catch (Exception e) {
      LOGGER.error("unexpected error when process engine {}:", engine, e);
      return null;
    } finally {
      try {
        if (needRelease) {
          storage.release();
        }
      } catch (Exception e) {
        LOGGER.error("release session pool failure!");
      }
    }
    return new Pair<>(new ColumnsInterval(null, null), new KeyInterval(0, Long.MAX_VALUE));
  }

  private boolean initStorage(StorageEngineMeta meta) {
    StorageEngineType engine = meta.getStorageEngine();
    String driver = drivers.get(engine);
    long id = meta.getId();
    try {
      if (!storageMap.containsKey(id)) {
        ClassLoader loader = classLoaders.get(engine);
        IStorage storage =
            (IStorage)
                loader.loadClass(driver).getConstructor(StorageEngineMeta.class).newInstance(meta);
        return initStorage(meta, storage);
      }
    } catch (ClassNotFoundException e) {
      LOGGER.error("load class {} for engine {} failure: {}", driver, engine, e);
      return false;
    } catch (Exception e) {
      LOGGER.error("unexpected error when process engine {}: ", engine, e);
      return false;
    }
    return true;
  }

  private boolean initStorage(StorageEngineMeta meta, IStorage storage) {
    StorageEngineType engine = meta.getStorageEngine();
    long id = meta.getId();
    try {
      if (!storageMap.containsKey(id)) {
        // 启动一个派发线程池
        ThreadPoolExecutor dispatcher =
            new ThreadPoolExecutor(
                ConfigDescriptor.getInstance()
                    .getConfig()
                    .getPhysicalTaskThreadPoolSizePerStorage(),
                Integer.MAX_VALUE,
                60L,
                TimeUnit.SECONDS,
                new SynchronousQueue<>());
        storageMap.put(meta.getId(), new Pair<>(storage, dispatcher));
      }
    } catch (Exception e) {
      LOGGER.error("unexpected error when process engine {}: {}", engine, e);
      return false;
    }
    return true;
  }

  private static void initClassLoaderAndDrivers() {
    if (hasInitLoaders) {
      return;
    }
    String[] parts = ConfigDescriptor.getInstance().getConfig().getDatabaseClassNames().split(",");
    for (String part : parts) {
      String[] kAndV = part.split("=");
      if (kAndV.length != 2) {
        LOGGER.error("unexpected database class names: {}", part);
        System.exit(-1);
      }
      String storage = kAndV[0];
      String driver = kAndV[1];
      try {
        ClassLoader classLoader = new StorageEngineClassLoader(storage);
        StorageEngineType type = StorageEngineType.valueOf(storage.toLowerCase());
        classLoaders.put(type, classLoader);
        drivers.put(type, driver);
      } catch (IOException e) {
        LOGGER.error("encounter error when init class loader for {}: {}", storage, e);
        System.exit(-1);
      }
    }
    hasInitLoaders = true;
  }

  public Map<Long, Pair<IStorage, ThreadPoolExecutor>> getStorageMap() {
    return storageMap;
  }

  public Pair<IStorage, ThreadPoolExecutor> getStorage(long id) {
    return storageMap.get(id);
  }

  public boolean addStorage(StorageEngineMeta meta) {
    if (!initStorage(meta)) {
      LOGGER.error("add storage {} failure!", meta);
      return false;
    } else {
      LOGGER.info("add storage {} success.", meta);
    }
    return true;
  }

  public boolean addStorage(StorageEngineMeta meta, IStorage storage) {
    if (!initStorage(meta, storage)) {
      LOGGER.error("add storage {} failure!", meta);
      return false;
    } else {
      LOGGER.info("add storage {} success.", meta);
    }
    return true;
  }

  public static IStorage initStorageInstance(StorageEngineMeta meta) {
    StorageEngineType engine = meta.getStorageEngine();
    String driver = drivers.get(engine);
    ClassLoader loader = classLoaders.get(engine);
    try {
      return (IStorage)
          loader.loadClass(driver).getConstructor(StorageEngineMeta.class).newInstance(meta);
    } catch (ClassNotFoundException e) {
      LOGGER.error("load class {} for engine {} failure: {}", driver, engine, e);
      return null;
    } catch (Exception e) {
      LOGGER.error("add storage {} failure!", meta);
      return null;
    }
  }
}
