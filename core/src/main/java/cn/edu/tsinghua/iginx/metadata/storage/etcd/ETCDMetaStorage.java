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
package cn.edu.tsinghua.iginx.metadata.storage.etcd;

import static cn.edu.tsinghua.iginx.metadata.storage.constant.Constant.*;
import static cn.edu.tsinghua.iginx.metadata.utils.ColumnsIntervalUtils.fromString;
import static cn.edu.tsinghua.iginx.metadata.utils.ReshardStatus.*;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.metadata.cache.IMetaCache;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.metadata.exception.MetaStorageException;
import cn.edu.tsinghua.iginx.metadata.hook.*;
import cn.edu.tsinghua.iginx.metadata.storage.IMetaStorage;
import cn.edu.tsinghua.iginx.metadata.utils.ReshardStatus;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.transform.pojo.TriggerDescriptor;
import cn.edu.tsinghua.iginx.utils.JsonUtils;
import cn.edu.tsinghua.iginx.utils.Pair;
import io.etcd.jetcd.*;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ETCDMetaStorage implements IMetaStorage {

  private static final Logger LOGGER = LoggerFactory.getLogger(ETCDMetaStorage.class);

  private static final String IGINX_ID = "/id/iginx";

  private static final String STORAGE_ID = "/id/storage";

  private static final String STORAGE_UNIT_ID = "/id/unit";

  private static final String FRAGMENT_REQUESTS_COUNTER_LOCK_NODE =
      "/lock/counter/fragment/requests";

  private static final long MAX_LOCK_TIME = 30; // 最长锁住 30 秒

  private static final long HEART_BEAT_INTERVAL = 5; // 和 etcd 之间的心跳包的时间间隔

  private static ETCDMetaStorage INSTANCE = null;

  private final Lock iginxLeaseLock = new ReentrantLock();
  private final Lock iginxConnectionLock = new ReentrantLock();
  private final Lock storageLeaseLock = new ReentrantLock();
  private final Lock storageConnectionLock = new ReentrantLock();
  private final Lock storageUnitLeaseLock = new ReentrantLock();
  private final Lock fragmentLeaseLock = new ReentrantLock();
  private final Lock userLeaseLock = new ReentrantLock();
  private final Lock transformLeaseLock = new ReentrantLock();
  private final Lock jobTriggerLeaseLock = new ReentrantLock();
  private final Lock fragmentRequestsCounterLeaseLock = new ReentrantLock();
  private final Lock fragmentHeatCounterLeaseLock = new ReentrantLock();
  private final Lock timeseriesHeatCounterLeaseLock = new ReentrantLock();
  private final Lock reshardStatusLeaseLock = new ReentrantLock();
  private final Lock reshardCounterLeaseLock = new ReentrantLock();
  private final Lock maxActiveEndTimeStatisticsLeaseLock = new ReentrantLock();

  private Client client;

  private Watch.Watcher iginxWatcher;
  private IginxChangeHook iginxChangeHook = null;
  private long iginxLease = -1L;
  private long iginxConnectionLease = -1L;
  private Watch.Watcher storageWatcher;
  private StorageChangeHook storageChangeHook = null;
  private long storageLease = -1L;
  private long storageConnectionLease = -1L;
  private Watch.Watcher storageUnitWatcher;
  private StorageUnitChangeHook storageUnitChangeHook = null;
  private long storageUnitLease = -1L;
  private Watch.Watcher fragmentWatcher;
  private FragmentChangeHook fragmentChangeHook = null;
  private long fragmentLease = -1L;
  private Watch.Watcher userWatcher;
  private UserChangeHook userChangeHook = null;
  private long userLease = -1L;
  private Watch.Watcher transformWatcher;
  private TransformChangeHook transformChangeHook = null;
  private JobTriggerChangeHook jobTriggerChangeHook = null;
  private long transformLease = -1L;

  private long jobTriggerLease = -1L;

  private long fragmentRequestsCounterLease = -1L;

  private long fragmentHeatCounterLease = -1L;

  private long timeseriesHeatCounterLease = -1L;

  private Watch.Watcher reshardStatusWatcher;
  private ReshardStatusChangeHook reshardStatusChangeHook = null;
  private long reshardStatusLease = -1L;
  private Watch.Watcher reshardCounterWatcher;
  private ReshardCounterChangeHook reshardCounterChangeHook = null;
  private long reshardCounterLease = -1L;
  private Watch.Watcher maxActiveEndTimeStatisticsWatcher;
  private MaxActiveEndKeyStatisticsChangeHook maxActiveEndKeyStatisticsChangeHook = null;
  private long maxActiveEndTimeStatisticsLease = -1L;

  private final int IGINX_NODE_LENGTH = 7;

  private final int STORAGE_ENGINE_NODE_LENGTH = 10;

  private String generateID(String prefix, long idLength, long val) {
    return String.format(prefix + "%0" + idLength + "d", val);
  }

  public ETCDMetaStorage() {
    client =
        Client.builder()
            .endpoints(ConfigDescriptor.getInstance().getConfig().getEtcdEndpoints().split(","))
            .build();

    // 注册 iginx 的监听
    this.iginxWatcher =
        client
            .getWatchClient()
            .watch(
                ByteSequence.from(IGINX_INFO_NODE_PREFIX.getBytes()),
                WatchOption.newBuilder()
                    .withPrefix(ByteSequence.from(IGINX_INFO_NODE_PREFIX.getBytes()))
                    .withPrevKV(true)
                    .build(),
                new Watch.Listener() {
                  @Override
                  public void onNext(WatchResponse watchResponse) {
                    if (ETCDMetaStorage.this.iginxChangeHook == null) {
                      return;
                    }
                    for (WatchEvent event : watchResponse.getEvents()) {
                      IginxMeta iginx;
                      switch (event.getEventType()) {
                        case PUT:
                          iginx =
                              JsonUtils.fromJson(
                                  event.getKeyValue().getValue().getBytes(), IginxMeta.class);
                          LOGGER.info(
                              "new iginx comes to cluster: id = {} ,ip = {} , port = {}",
                              iginx.getId(),
                              iginx.getIp(),
                              iginx.getPort());
                          ETCDMetaStorage.this.iginxChangeHook.onChange(iginx.getId(), iginx);
                          break;
                        case DELETE:
                          iginx =
                              JsonUtils.fromJson(
                                  event.getPrevKV().getValue().getBytes(), IginxMeta.class);
                          LOGGER.info(
                              "iginx leave from cluster: id = {} ,ip = {} , port = {}",
                              iginx.getId(),
                              iginx.getIp(),
                              iginx.getPort());
                          iginxChangeHook.onChange(iginx.getId(), null);
                          break;
                        default:
                          LOGGER.error("unexpected watchEvent: {}", event.getEventType());
                          break;
                      }
                    }
                  }

                  @Override
                  public void onError(Throwable throwable) {}

                  @Override
                  public void onCompleted() {}
                });

    // 注册 storage 的监听
    this.storageWatcher =
        client
            .getWatchClient()
            .watch(
                ByteSequence.from(STORAGE_ENGINE_NODE_PREFIX.getBytes()),
                WatchOption.newBuilder()
                    .withPrefix(ByteSequence.from(STORAGE_ENGINE_NODE_PREFIX.getBytes()))
                    .withPrevKV(true)
                    .build(),
                new Watch.Listener() {
                  @Override
                  public void onNext(WatchResponse watchResponse) {
                    if (ETCDMetaStorage.this.storageWatcher == null) {
                      return;
                    }
                    for (WatchEvent event : watchResponse.getEvents()) {
                      StorageEngineMeta storageEngine;
                      switch (event.getEventType()) {
                        case PUT:
                          storageEngine =
                              JsonUtils.fromJson(
                                  event.getKeyValue().getValue().getBytes(),
                                  StorageEngineMeta.class);
                          LOGGER.info("storage engine meta updated {}", storageEngine);
                          storageChangeHook.onChange(storageEngine.getId(), storageEngine);
                          break;
                        case DELETE:
                          storageEngine =
                              JsonUtils.fromJson(
                                  event.getPrevKV().getValue().getBytes(), StorageEngineMeta.class);
                          LOGGER.info(
                              "storage engine leave from cluster: id = {} ,ip = {} , port = {}",
                              storageEngine.getId(),
                              storageEngine.getIp(),
                              storageEngine.getPort());
                          storageChangeHook.onChange(storageEngine.getId(), null);
                          break;
                        default:
                          LOGGER.error("unexpected watchEvent: {}", event.getEventType());
                          break;
                      }
                    }
                  }

                  @Override
                  public void onError(Throwable throwable) {}

                  @Override
                  public void onCompleted() {}
                });

    // 注册 storage unit 的监听
    this.storageUnitWatcher =
        client
            .getWatchClient()
            .watch(
                ByteSequence.from(STORAGE_UNIT_NODE_PREFIX.getBytes()),
                WatchOption.newBuilder()
                    .withPrefix(ByteSequence.from(STORAGE_UNIT_NODE_PREFIX.getBytes()))
                    .withPrevKV(true)
                    .build(),
                new Watch.Listener() {
                  @Override
                  public void onNext(WatchResponse watchResponse) {
                    if (ETCDMetaStorage.this.storageUnitWatcher == null) {
                      return;
                    }
                    for (WatchEvent event : watchResponse.getEvents()) {
                      StorageUnitMeta storageUnit;
                      switch (event.getEventType()) {
                        case PUT:
                          storageUnit =
                              JsonUtils.fromJson(
                                  event.getKeyValue().getValue().getBytes(), StorageUnitMeta.class);
                          storageUnitChangeHook.onChange(storageUnit.getId(), storageUnit);
                          break;
                        case DELETE:
                        default:
                          LOGGER.error("unexpected watchEvent: {}", event.getEventType());
                          break;
                      }
                    }
                  }

                  @Override
                  public void onError(Throwable throwable) {}

                  @Override
                  public void onCompleted() {}
                });

    // 注册 fragment 的监听
    this.fragmentWatcher =
        client
            .getWatchClient()
            .watch(
                ByteSequence.from(FRAGMENT_NODE_PREFIX.getBytes()),
                WatchOption.newBuilder()
                    .withPrefix(ByteSequence.from(FRAGMENT_NODE_PREFIX.getBytes()))
                    .withPrevKV(true)
                    .build(),
                new Watch.Listener() {
                  @Override
                  public void onNext(WatchResponse watchResponse) {
                    if (ETCDMetaStorage.this.fragmentChangeHook == null) {
                      return;
                    }
                    for (WatchEvent event : watchResponse.getEvents()) {
                      FragmentMeta fragment;
                      switch (event.getEventType()) {
                        case PUT:
                          fragment =
                              JsonUtils.fromJson(
                                  event.getKeyValue().getValue().getBytes(), FragmentMeta.class);
                          boolean isCreate =
                              event.getPrevKV().getVersion() == 0; // 上一次如果是 0，意味着就是创建
                          fragmentChangeHook.onChange(isCreate, fragment);
                          break;
                        case DELETE:
                        default:
                          LOGGER.error("unexpected watchEvent: {}", event.getEventType());
                          break;
                      }
                    }
                  }

                  @Override
                  public void onError(Throwable throwable) {}

                  @Override
                  public void onCompleted() {}
                });

    // 注册 user 的监听
    this.userWatcher =
        client
            .getWatchClient()
            .watch(
                ByteSequence.from(USER_NODE_PREFIX.getBytes()),
                WatchOption.newBuilder()
                    .withPrefix(ByteSequence.from(USER_NODE_PREFIX.getBytes()))
                    .withPrevKV(true)
                    .build(),
                new Watch.Listener() {
                  @Override
                  public void onNext(WatchResponse watchResponse) {
                    if (ETCDMetaStorage.this.userChangeHook == null) {
                      return;
                    }
                    for (WatchEvent event : watchResponse.getEvents()) {
                      UserMeta userMeta;
                      switch (event.getEventType()) {
                        case PUT:
                          userMeta =
                              JsonUtils.fromJson(
                                  event.getKeyValue().getValue().getBytes(), UserMeta.class);
                          userChangeHook.onChange(userMeta.getUsername(), userMeta);
                          break;
                        case DELETE:
                          userMeta =
                              JsonUtils.fromJson(
                                  event.getPrevKV().getValue().getBytes(), UserMeta.class);
                          userChangeHook.onChange(userMeta.getUsername(), null);
                          break;
                        default:
                          LOGGER.error("unexpected watchEvent: {}", event.getEventType());
                          break;
                      }
                    }
                  }

                  @Override
                  public void onError(Throwable throwable) {}

                  @Override
                  public void onCompleted() {}
                });

    // 注册 transform 的监听
    this.transformWatcher =
        client
            .getWatchClient()
            .watch(
                ByteSequence.from(TRANSFORM_NODE_PREFIX.getBytes()),
                WatchOption.newBuilder()
                    .withPrefix(ByteSequence.from(TRANSFORM_NODE_PREFIX.getBytes()))
                    .withPrevKV(true)
                    .build(),
                new Watch.Listener() {
                  @Override
                  public void onNext(WatchResponse watchResponse) {
                    if (ETCDMetaStorage.this.transformChangeHook == null) {
                      return;
                    }
                    for (WatchEvent event : watchResponse.getEvents()) {
                      TransformTaskMeta taskMeta;
                      switch (event.getEventType()) {
                        case PUT:
                          taskMeta =
                              JsonUtils.fromJson(
                                  event.getKeyValue().getValue().getBytes(),
                                  TransformTaskMeta.class);
                          transformChangeHook.onChange(taskMeta.getName(), taskMeta);
                          break;
                        case DELETE:
                          taskMeta =
                              JsonUtils.fromJson(
                                  event.getPrevKV().getValue().getBytes(), TransformTaskMeta.class);
                          transformChangeHook.onChange(taskMeta.getName(), null);
                          break;
                        default:
                          LOGGER.error("unexpected watchEvent: {}", event.getEventType());
                          break;
                      }
                    }
                  }

                  @Override
                  public void onError(Throwable throwable) {}

                  @Override
                  public void onCompleted() {}
                });

    // 注册 reshardStatus 的监听
    this.reshardStatusWatcher =
        client
            .getWatchClient()
            .watch(
                ByteSequence.from(RESHARD_STATUS_NODE_PREFIX.getBytes()),
                WatchOption.newBuilder()
                    .withPrefix(ByteSequence.from(RESHARD_STATUS_NODE_PREFIX.getBytes()))
                    .withPrevKV(true)
                    .build(),
                new Watch.Listener() {
                  @Override
                  public void onNext(WatchResponse watchResponse) {
                    if (ETCDMetaStorage.this.reshardStatusChangeHook == null) {
                      return;
                    }
                    for (WatchEvent event : watchResponse.getEvents()) {
                      ReshardStatus status;
                      switch (event.getEventType()) {
                        case PUT:
                          status =
                              JsonUtils.fromJson(
                                  event.getKeyValue().getValue().getBytes(), ReshardStatus.class);
                          reshardStatusChangeHook.onChange(status);
                          break;
                        case DELETE:
                          status =
                              JsonUtils.fromJson(
                                  event.getPrevKV().getValue().getBytes(), ReshardStatus.class);
                          reshardStatusChangeHook.onChange(status);
                          break;
                        default:
                          LOGGER.error("unexpected watchEvent: {}", event.getEventType());
                          break;
                      }
                    }
                  }

                  @Override
                  public void onError(Throwable throwable) {}

                  @Override
                  public void onCompleted() {}
                });

    // 注册 reshardCounter 的监听
    this.reshardCounterWatcher =
        client
            .getWatchClient()
            .watch(
                ByteSequence.from(RESHARD_COUNTER_NODE_PREFIX.getBytes()),
                WatchOption.newBuilder()
                    .withPrefix(ByteSequence.from(RESHARD_COUNTER_NODE_PREFIX.getBytes()))
                    .withPrevKV(true)
                    .build(),
                new Watch.Listener() {
                  @Override
                  public void onNext(WatchResponse watchResponse) {
                    if (ETCDMetaStorage.this.reshardCounterChangeHook == null) {
                      return;
                    }
                    for (WatchEvent event : watchResponse.getEvents()) {
                      int counter;
                      switch (event.getEventType()) {
                        case PUT:
                          counter =
                              JsonUtils.fromJson(
                                  event.getKeyValue().getValue().getBytes(), Integer.class);
                          reshardCounterChangeHook.onChange(counter);
                          break;
                        case DELETE:
                          counter =
                              JsonUtils.fromJson(
                                  event.getPrevKV().getValue().getBytes(), Integer.class);
                          reshardCounterChangeHook.onChange(counter);
                          break;
                        default:
                          LOGGER.error("unexpected watchEvent: {}", event.getEventType());
                          break;
                      }
                    }
                  }

                  @Override
                  public void onError(Throwable throwable) {}

                  @Override
                  public void onCompleted() {}
                });

    // 注册 maxActiveEndTimeStatistics 的监听
    this.maxActiveEndTimeStatisticsWatcher =
        client
            .getWatchClient()
            .watch(
                ByteSequence.from(MAX_ACTIVE_END_TIME_STATISTICS_NODE_PREFIX.getBytes()),
                WatchOption.newBuilder()
                    .withPrefix(
                        ByteSequence.from(MAX_ACTIVE_END_TIME_STATISTICS_NODE_PREFIX.getBytes()))
                    .withPrevKV(true)
                    .build(),
                new Watch.Listener() {
                  @Override
                  public void onNext(WatchResponse watchResponse) {
                    if (ETCDMetaStorage.this.maxActiveEndKeyStatisticsChangeHook == null) {
                      return;
                    }
                    for (WatchEvent event : watchResponse.getEvents()) {
                      long endTime;
                      switch (event.getEventType()) {
                        case PUT:
                          endTime =
                              JsonUtils.fromJson(
                                  event.getKeyValue().getValue().getBytes(), Long.class);
                          maxActiveEndKeyStatisticsChangeHook.onChange(endTime);
                          break;
                        case DELETE:
                          endTime =
                              JsonUtils.fromJson(
                                  event.getPrevKV().getValue().getBytes(), Long.class);
                          maxActiveEndKeyStatisticsChangeHook.onChange(endTime);
                          break;
                        default:
                          LOGGER.error("unexpected watchEvent: {}", event.getEventType());
                          break;
                      }
                    }
                  }

                  @Override
                  public void onError(Throwable throwable) {}

                  @Override
                  public void onCompleted() {}
                });
  }

  public static ETCDMetaStorage getInstance() {
    if (INSTANCE == null) {
      synchronized (ETCDMetaStorage.class) {
        if (INSTANCE == null) {
          INSTANCE = new ETCDMetaStorage();
        }
      }
    }
    return INSTANCE;
  }

  private long nextId(String category) throws InterruptedException, ExecutionException {
    return client
        .getKVClient()
        .put(
            ByteSequence.from(category.getBytes()),
            ByteSequence.EMPTY,
            PutOption.newBuilder().withPrevKV().build())
        .get()
        .getPrevKv()
        .getVersion();
  }

  private void lockIginx() throws MetaStorageException {
    try {
      iginxLeaseLock.lock();
      iginxLease = client.getLeaseClient().grant(MAX_LOCK_TIME).get().getID();
      client.getLockClient().lock(ByteSequence.from(IGINX_LOCK_NODE.getBytes()), iginxLease);
    } catch (Exception e) {
      iginxLeaseLock.unlock();
      throw new MetaStorageException("acquire iginx mutex error: ", e);
    }
  }

  private void releaseIginx() throws MetaStorageException {
    try {
      client.getLockClient().unlock(ByteSequence.from(IGINX_LOCK_NODE.getBytes())).get();
      client.getLeaseClient().revoke(iginxLease).get();
      iginxLease = -1L;
    } catch (Exception e) {
      throw new MetaStorageException("release iginx error: ", e);
    } finally {
      iginxLeaseLock.unlock();
    }
  }

  @Override
  public Map<Long, IginxMeta> loadIginx() throws MetaStorageException {
    try {
      lockIginx();
      Map<Long, IginxMeta> iginxMap = new HashMap<>();
      GetResponse response =
          this.client
              .getKVClient()
              .get(
                  ByteSequence.from(IGINX_INFO_NODE_PREFIX.getBytes()),
                  GetOption.newBuilder()
                      .withPrefix(ByteSequence.from(IGINX_INFO_NODE_PREFIX.getBytes()))
                      .build())
              .get();
      response
          .getKvs()
          .forEach(
              e -> {
                IginxMeta iginx = JsonUtils.fromJson(e.getValue().getBytes(), IginxMeta.class);
                iginxMap.put(iginx.getId(), iginx);
              });
      return iginxMap;
    } catch (ExecutionException | InterruptedException e) {
      LOGGER.error("got error when load iginx: ", e);
      throw new MetaStorageException(e);
    } finally {
      if (iginxLease != -1) {
        releaseIginx();
      }
    }
  }

  @Override
  public IginxMeta registerIginx(IginxMeta iginx) throws MetaStorageException {
    try {
      lockIginx();
      // 申请一个 id
      long id = nextId(IGINX_ID);
      Lease lease = this.client.getLeaseClient();
      long iginxLeaseId = lease.grant(HEART_BEAT_INTERVAL).get().getID();
      lease.keepAlive(
          iginxLeaseId,
          new StreamObserver<LeaseKeepAliveResponse>() {
            @Override
            public void onNext(LeaseKeepAliveResponse leaseKeepAliveResponse) {
              LOGGER.debug("send heart beat to etcd succeed.");
            }

            @Override
            public void onError(Throwable throwable) {
              LOGGER.error("got error when send heart beat to etcd: ", throwable);
            }

            @Override
            public void onCompleted() {}
          });
      iginx = new IginxMeta(id, iginx.getIp(), iginx.getPort(), iginx.getExtraParams());
      this.client
          .getKVClient()
          .put(
              ByteSequence.from(
                  generateID(IGINX_INFO_NODE_PREFIX, IGINX_NODE_LENGTH, id).getBytes()),
              ByteSequence.from(JsonUtils.toJson(iginx)))
          .get();
      return iginx;
    } catch (ExecutionException | InterruptedException e) {
      LOGGER.error("got error when register iginx meta: ", e);
      throw new MetaStorageException(e);
    } finally {
      if (iginxLease != -1) {
        releaseIginx();
      }
    }
  }

  @Override
  public void registerIginxChangeHook(IginxChangeHook hook) {
    this.iginxChangeHook = hook;
  }

  private void lockStorage() throws MetaStorageException {
    try {
      storageLeaseLock.lock();
      storageLease = client.getLeaseClient().grant(MAX_LOCK_TIME).get().getID();
      client
          .getLockClient()
          .lock(ByteSequence.from(STORAGE_ENGINE_LOCK_NODE.getBytes()), storageLease);
    } catch (Exception e) {
      storageLeaseLock.unlock();
      throw new MetaStorageException("acquire storage mutex error: ", e);
    }
  }

  private void releaseStorage() throws MetaStorageException {
    try {
      client.getLockClient().unlock(ByteSequence.from(STORAGE_ENGINE_LOCK_NODE.getBytes())).get();
      client.getLeaseClient().revoke(storageLease).get();
      storageLease = -1L;
    } catch (Exception e) {
      throw new MetaStorageException("release storage error: ", e);
    } finally {
      storageLeaseLock.unlock();
    }
  }

  @Override
  public Map<Long, StorageEngineMeta> loadStorageEngine(List<StorageEngineMeta> localStorageEngines)
      throws MetaStorageException {
    try {
      lockStorage();
      Map<Long, StorageEngineMeta> storageEngines = new HashMap<>();
      GetResponse response =
          this.client
              .getKVClient()
              .get(
                  ByteSequence.from(STORAGE_ENGINE_NODE_PREFIX.getBytes()),
                  GetOption.newBuilder()
                      .withPrefix(ByteSequence.from(STORAGE_ENGINE_NODE_PREFIX.getBytes()))
                      .build())
              .get();
      if (response.getCount() != 0L) { // 服务器上已经有了，本地的不作数
        response
            .getKvs()
            .forEach(
                e -> {
                  StorageEngineMeta storageEngine =
                      JsonUtils.fromJson(e.getValue().getBytes(), StorageEngineMeta.class);
                  storageEngines.put(storageEngine.getId(), storageEngine);
                });
      } else { // 服务器上还没有，将本地的注册到服务器上
        for (StorageEngineMeta storageEngine : localStorageEngines) {
          long id = nextId(STORAGE_ID); // 给每个数据后端分配 id
          storageEngine.setId(id);
          storageEngines.put(storageEngine.getId(), storageEngine);
          this.client
              .getKVClient()
              .put(
                  ByteSequence.from(
                      generateID(STORAGE_ENGINE_NODE_PREFIX, STORAGE_ENGINE_NODE_LENGTH, id)
                          .getBytes()),
                  ByteSequence.from(JsonUtils.toJson(storageEngine)))
              .get();
        }
      }
      return storageEngines;
    } catch (ExecutionException | InterruptedException e) {
      LOGGER.error("got error when load storage: ", e);
      throw new MetaStorageException(e);
    } finally {
      if (storageLease != -1) {
        releaseStorage();
      }
    }
  }

  @Override
  public long addStorageEngine(long iginxId, StorageEngineMeta storageEngine)
      throws MetaStorageException {
    try {
      lockStorage();
      lockStorageConnection();
      long id = nextId(STORAGE_ID);
      storageEngine.setId(id);
      this.client
          .getKVClient()
          .put(
              ByteSequence.from(
                  generateID(STORAGE_ENGINE_NODE_PREFIX, STORAGE_ENGINE_NODE_LENGTH, id)
                      .getBytes()),
              ByteSequence.from(JsonUtils.toJson(storageEngine)))
          .get();

      // 记录连接关系
      String connectionPath =
          generateID(STORAGE_CONNECTION_NODE_PREFIX, IGINX_NODE_LENGTH, iginxId);
      GetResponse response =
          this.client
              .getKVClient()
              .get(
                  ByteSequence.from(connectionPath.getBytes()),
                  GetOption.newBuilder()
                      .withPrefix(ByteSequence.from(STORAGE_CONNECTION_NODE_PREFIX.getBytes()))
                      .build())
              .get();
      long[] ids;
      if (response.getCount() != 1) {
        ids = new long[1];
        ids[0] = id;
      } else {
        long[] oldIds =
            JsonUtils.fromJson(response.getKvs().get(0).getValue().getBytes(), long[].class);
        ids = new long[oldIds.length + 1];
        System.arraycopy(oldIds, 0, ids, 0, oldIds.length);
        ids[oldIds.length] = id;
      }
      this.client
          .getKVClient()
          .put(
              ByteSequence.from(connectionPath.getBytes()),
              ByteSequence.from(JsonUtils.toJson(ids)))
          .get();
      return id;
    } catch (ExecutionException | InterruptedException e) {
      LOGGER.error("got error when add storage: ", e);
      throw new MetaStorageException(e);
    } finally {
      if (storageLease != -1) {
        releaseStorage();
      }
      if (storageConnectionLease != -1) {
        releaseStorageConnection();
      }
    }
  }

  @Override
  public void removeDummyStorageEngine(long iginxId, long storageEngineId, boolean forAllIginx)
      throws MetaStorageException {
    try {
      lockStorage();
      lockStorageConnection();
      if (forAllIginx) {
        this.client
            .getKVClient()
            .delete(
                ByteSequence.from(
                    generateID(
                            STORAGE_ENGINE_NODE_PREFIX, STORAGE_ENGINE_NODE_LENGTH, storageEngineId)
                        .getBytes()));
      }

      // 删除连接状态
      String connectionPath =
          generateID(STORAGE_CONNECTION_NODE_PREFIX, IGINX_NODE_LENGTH, iginxId);
      GetResponse response =
          this.client
              .getKVClient()
              .get(
                  ByteSequence.from(connectionPath.getBytes()),
                  GetOption.newBuilder()
                      .withPrefix(ByteSequence.from(STORAGE_CONNECTION_NODE_PREFIX.getBytes()))
                      .build())
              .get();
      if (response.getCount() != 0L) {
        long[] ids =
            JsonUtils.fromJson(response.getKvs().get(0).getValue().getBytes(), long[].class);
        int index = Arrays.binarySearch(ids, storageEngineId);
        if (index >= 0) {
          if (ids.length > 1) {
            long[] newIds = new long[ids.length - 1];
            System.arraycopy(ids, 0, newIds, 0, index);
            System.arraycopy(ids, index + 1, newIds, index, ids.length - index - 1);
            this.client
                .getKVClient()
                .put(
                    ByteSequence.from(connectionPath.getBytes()),
                    ByteSequence.from(JsonUtils.toJson(newIds)))
                .get();
          } else {
            // iginx 将没有连接的存储节点
            this.client.getKVClient().delete(ByteSequence.from(connectionPath.getBytes()));
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("got error when removing dummy storage engine: ", e);
      throw new MetaStorageException(e);
    } finally {
      if (storageLease != -1) {
        releaseStorage();
      }
      if (storageConnectionLease != -1) {
        releaseStorageConnection();
      }
    }
  }

  @Override
  public void registerStorageChangeHook(StorageChangeHook hook) {
    this.storageChangeHook = hook;
  }

  private void lockIginxConnection() throws MetaStorageException {
    try {
      iginxConnectionLock.lock();
      iginxConnectionLease = client.getLeaseClient().grant(MAX_LOCK_TIME).get().getID();
      client
          .getLockClient()
          .lock(ByteSequence.from(IGINX_CONNECTION_LOCK_NODE.getBytes()), iginxConnectionLease);
    } catch (Exception e) {
      iginxConnectionLock.unlock();
      throw new MetaStorageException("acquire iginx connection mutex error: ", e);
    }
  }

  private void releaseIginxConnection() throws MetaStorageException {
    try {
      client.getLockClient().unlock(ByteSequence.from(IGINX_CONNECTION_LOCK_NODE.getBytes())).get();
      client.getLeaseClient().revoke(iginxConnectionLease).get();
      iginxConnectionLease = -1L;
    } catch (Exception e) {
      throw new MetaStorageException("release iginx connection error: ", e);
    } finally {
      iginxConnectionLock.unlock();
    }
  }

  @Override
  public void refreshAchievableIginx(IginxMeta iginx) throws MetaStorageException {
    try {
      lockIginx();
      lockIginxConnection();
      List<Long> achievableIds = new ArrayList<>();
      GetResponse response =
          this.client
              .getKVClient()
              .get(
                  ByteSequence.from(IGINX_INFO_NODE_PREFIX.getBytes()),
                  GetOption.newBuilder()
                      .withPrefix(ByteSequence.from(IGINX_INFO_NODE_PREFIX.getBytes()))
                      .build())
              .get();
      response
          .getKvs()
          .forEach(
              e -> {
                IginxMeta iginxMeta = JsonUtils.fromJson(e.getValue().getBytes(), IginxMeta.class);
                if (iginx.getIp().equals(iginxMeta.getIp())
                    && iginx.getPort() == iginxMeta.getPort()) {
                  return;
                }
                // 尝试与集群中其他iginx建立连接
                Session session = new Session(iginxMeta.iginxMetaInfo());
                try {
                  session.openSession();
                  LOGGER.info(
                      "connect to iginx(id = {} ,ip = {} , port = {})",
                      iginxMeta.getId(),
                      iginxMeta.getIp(),
                      iginxMeta.getPort());
                  achievableIds.add(iginxMeta.getId());
                  session.closeSession();
                } catch (SessionException exception) {
                  LOGGER.info(
                      "open session of iginx(id = {} ,ip = {} , port = {}) failed, because: ",
                      iginxMeta.getId(),
                      iginxMeta.getIp(),
                      iginxMeta.getPort(),
                      exception);
                }
              });

      long[] ids = achievableIds.stream().mapToLong(Long::longValue).toArray();
      this.client
          .getKVClient()
          .put(
              ByteSequence.from(
                  generateID(IGINX_CONNECTION_NODE_PREFIX, IGINX_NODE_LENGTH, iginx.getId())
                      .getBytes()),
              ByteSequence.from(JsonUtils.toJson(ids)))
          .get();
    } catch (ExecutionException | InterruptedException e) {
      LOGGER.error("got error when connect to other iginx: ", e);
      throw new MetaStorageException(e);
    } finally {
      if (iginxLease != -1) {
        releaseIginx();
      }
      if (iginxConnectionLease != -1) {
        releaseIginxConnection();
      }
    }
  }

  @Override
  public Map<Long, Set<Long>> refreshClusterIginxConnectivity() throws MetaStorageException {
    try {
      lockIginxConnection();
      Map<Long, Set<Long>> connectionMap = new HashMap<>();
      GetResponse response =
          this.client
              .getKVClient()
              .get(
                  ByteSequence.from(IGINX_CONNECTION_NODE_PREFIX.getBytes()),
                  GetOption.newBuilder()
                      .withPrefix(ByteSequence.from(IGINX_CONNECTION_NODE_PREFIX.getBytes()))
                      .build())
              .get();
      response
          .getKvs()
          .forEach(
              e -> {
                String keyName = e.getKey().toString(StandardCharsets.UTF_8);
                long fromId =
                    Long.parseLong(keyName.substring(IGINX_CONNECTION_NODE_PREFIX.length()));
                long[] toIds = JsonUtils.fromJson(e.getValue().getBytes(), long[].class);
                for (long toId : toIds) {
                  connectionMap.computeIfAbsent(fromId, k -> new HashSet<>()).add(toId);
                }
              });
      return connectionMap;
    } catch (Exception e) {
      throw new MetaStorageException("get error when update cluster storage connections", e);
    } finally {
      if (iginxConnectionLease != -1) {
        releaseIginxConnection();
      }
    }
  }

  private void lockStorageConnection() throws MetaStorageException {
    try {
      storageConnectionLock.lock();
      storageConnectionLease = client.getLeaseClient().grant(MAX_LOCK_TIME).get().getID();
      client
          .getLockClient()
          .lock(ByteSequence.from(STORAGE_CONNECTION_LOCK_NODE.getBytes()), storageConnectionLease);
    } catch (Exception e) {
      storageConnectionLock.unlock();
      throw new MetaStorageException("acquire storage connection mutex error: ", e);
    }
  }

  private void releaseStorageConnection() throws MetaStorageException {
    try {
      client
          .getLockClient()
          .unlock(ByteSequence.from(STORAGE_CONNECTION_LOCK_NODE.getBytes()))
          .get();
      client.getLeaseClient().revoke(storageConnectionLease).get();
      storageConnectionLease = -1L;
    } catch (Exception e) {
      throw new MetaStorageException("release storage connection error: ", e);
    } finally {
      storageConnectionLock.unlock();
    }
  }

  @Override
  public void addStorageConnection(long iginxId, List<StorageEngineMeta> storageEngines)
      throws MetaStorageException {
    try {
      lockStorage();
      lockStorageConnection();
      for (StorageEngineMeta storageEngine : storageEngines) {
        GetResponse response =
            this.client
                .getKVClient()
                .get(
                    ByteSequence.from(
                        generateID(
                                STORAGE_ENGINE_NODE_PREFIX,
                                STORAGE_ENGINE_NODE_LENGTH,
                                storageEngine.getId())
                            .getBytes()))
                .get();
        if (response.getCount() != 1) {
          LOGGER.error("storage engine {} does not exist", storageEngine.getId());
          return;
        } else {
          LOGGER.info("connect to storage engine {} ", storageEngine);
        }
      }
      // 记录连接关系
      String connectionPath =
          generateID(STORAGE_CONNECTION_NODE_PREFIX, IGINX_NODE_LENGTH, iginxId);
      GetResponse response =
          this.client
              .getKVClient()
              .get(
                  ByteSequence.from(connectionPath.getBytes()),
                  GetOption.newBuilder()
                      .withPrefix(ByteSequence.from(STORAGE_CONNECTION_NODE_PREFIX.getBytes()))
                      .build())
              .get();
      long[] ids, oldIds;
      if (response.getCount() != 1) {
        oldIds = new long[0];
      } else {
        oldIds = JsonUtils.fromJson(response.getKvs().get(0).getValue().getBytes(), long[].class);
      }
      ids = new long[oldIds.length + storageEngines.size()];
      System.arraycopy(oldIds, 0, ids, 0, oldIds.length);
      for (int i = 0; i < storageEngines.size(); i++) {
        ids[oldIds.length + i] = storageEngines.get(i).getId();
      }
      this.client
          .getKVClient()
          .put(
              ByteSequence.from(connectionPath.getBytes()),
              ByteSequence.from(JsonUtils.toJson(ids)))
          .get();
    } catch (Exception e) {
      throw new MetaStorageException("get error when add storage connection", e);
    } finally {
      if (storageLease != -1) {
        releaseStorage();
      }
      if (storageConnectionLease != -1) {
        releaseStorageConnection();
      }
    }
  }

  @Override
  public Map<Long, Set<Long>> refreshClusterStorageConnections() throws MetaStorageException {
    try {
      lockStorageConnection();
      Map<Long, Set<Long>> connectionMap = new HashMap<>();
      GetResponse response =
          this.client
              .getKVClient()
              .get(
                  ByteSequence.from(STORAGE_CONNECTION_NODE_PREFIX.getBytes()),
                  GetOption.newBuilder()
                      .withPrefix(ByteSequence.from(STORAGE_CONNECTION_NODE_PREFIX.getBytes()))
                      .build())
              .get();
      response
          .getKvs()
          .forEach(
              e -> {
                String keyName = e.getKey().toString(StandardCharsets.UTF_8);
                long fromId =
                    Long.parseLong(keyName.substring(STORAGE_CONNECTION_NODE_PREFIX.length()));
                long[] toIds = JsonUtils.fromJson(e.getValue().getBytes(), long[].class);
                for (long toId : toIds) {
                  connectionMap.computeIfAbsent(fromId, k -> new HashSet<>()).add(toId);
                }
              });
      return connectionMap;
    } catch (Exception e) {
      throw new MetaStorageException("get error when update cluster storage connections", e);
    } finally {
      if (storageConnectionLease != -1) {
        releaseStorageConnection();
      }
    }
  }

  @Override
  public Map<String, StorageUnitMeta> loadStorageUnit() throws MetaStorageException {
    try {
      Map<String, StorageUnitMeta> storageUnitMap = new HashMap<>();
      GetResponse response =
          this.client
              .getKVClient()
              .get(
                  ByteSequence.from(STORAGE_UNIT_NODE_PREFIX.getBytes()),
                  GetOption.newBuilder()
                      .withPrefix(ByteSequence.from(STORAGE_UNIT_NODE_PREFIX.getBytes()))
                      .build())
              .get();
      List<KeyValue> kvs = response.getKvs();
      kvs.sort(Comparator.comparing(e -> e.getKey().toString(StandardCharsets.UTF_8)));
      for (KeyValue kv : kvs) {
        StorageUnitMeta storageUnit =
            JsonUtils.fromJson(kv.getValue().getBytes(), StorageUnitMeta.class);
        if (!storageUnit.isMaster()) { // 需要加入到主节点的子节点列表中
          StorageUnitMeta masterStorageUnit = storageUnitMap.get(storageUnit.getMasterId());
          if (masterStorageUnit == null) { // 子节点先于主节点加入系统中，不应该发生，报错
            LOGGER.error(
                "unexpected storage unit {}, because it does not has a master storage unit",
                new String(kv.getValue().getBytes()));
          } else {
            masterStorageUnit.addReplica(storageUnit);
          }
        }
        storageUnitMap.put(storageUnit.getId(), storageUnit);
      }
      return storageUnitMap;
    } catch (ExecutionException | InterruptedException e) {
      LOGGER.error("got error when load storage unit: ", e);
      throw new MetaStorageException(e);
    }
  }

  @Override
  public void lockStorageUnit() throws MetaStorageException {
    try {
      storageUnitLeaseLock.lock();
      storageUnitLease = client.getLeaseClient().grant(MAX_LOCK_TIME).get().getID();
      client
          .getLockClient()
          .lock(ByteSequence.from(STORAGE_UNIT_LOCK_NODE.getBytes()), storageUnitLease)
          .get()
          .getKey();
    } catch (Exception e) {
      storageUnitLeaseLock.unlock();
      throw new MetaStorageException("acquire storage unit mutex error: ", e);
    }
  }

  @Override
  public String addStorageUnit() throws MetaStorageException {
    try {
      return generateID("unit", STORAGE_ENGINE_NODE_LENGTH, nextId(STORAGE_UNIT_ID));
    } catch (InterruptedException | ExecutionException e) {
      throw new MetaStorageException("add storage unit error: ", e);
    }
  }

  @Override
  public void updateStorageUnit(StorageUnitMeta storageUnitMeta) throws MetaStorageException {
    try {
      client
          .getKVClient()
          .put(
              ByteSequence.from((STORAGE_UNIT_NODE_PREFIX + storageUnitMeta.getId()).getBytes()),
              ByteSequence.from(JsonUtils.toJson(storageUnitMeta)))
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new MetaStorageException("update storage unit error: ", e);
    }
  }

  @Override
  public void releaseStorageUnit() throws MetaStorageException {
    try {
      client.getLockClient().unlock(ByteSequence.from(STORAGE_UNIT_LOCK_NODE.getBytes())).get();
      client.getLeaseClient().revoke(storageUnitLease).get();
      storageUnitLease = -1L;
    } catch (Exception e) {
      throw new MetaStorageException("release storage mutex error: ", e);
    } finally {
      storageUnitLeaseLock.unlock();
    }
  }

  @Override
  public void registerStorageUnitChangeHook(StorageUnitChangeHook hook) {
    this.storageUnitChangeHook = hook;
  }

  @Override
  public Map<ColumnsInterval, List<FragmentMeta>> loadFragment() throws MetaStorageException {
    try {
      Map<ColumnsInterval, List<FragmentMeta>> fragmentsMap = new HashMap<>();
      GetResponse response =
          this.client
              .getKVClient()
              .get(
                  ByteSequence.from(FRAGMENT_NODE_PREFIX.getBytes()),
                  GetOption.newBuilder()
                      .withPrefix(ByteSequence.from(FRAGMENT_NODE_PREFIX.getBytes()))
                      .build())
              .get();
      for (KeyValue kv : response.getKvs()) {
        FragmentMeta fragment = JsonUtils.fromJson(kv.getValue().getBytes(), FragmentMeta.class);
        fragmentsMap
            .computeIfAbsent(fragment.getColumnsInterval(), e -> new ArrayList<>())
            .add(fragment);
      }
      return fragmentsMap;
    } catch (ExecutionException | InterruptedException e) {
      LOGGER.error("got error when load fragments: ", e);
      throw new MetaStorageException(e);
    }
  }

  @Override
  public void lockFragment() throws MetaStorageException {
    try {
      fragmentLeaseLock.lock();
      fragmentLease = client.getLeaseClient().grant(MAX_LOCK_TIME).get().getID();
      client.getLockClient().lock(ByteSequence.from(FRAGMENT_LOCK_NODE.getBytes()), fragmentLease);
    } catch (Exception e) {
      fragmentLeaseLock.unlock();
      throw new MetaStorageException("acquire fragment mutex error: ", e);
    }
  }

  @Override
  public List<FragmentMeta> getFragmentListByColumnNameAndKeyInterval(
      String columnName, KeyInterval keyInterval) {
    try {
      List<FragmentMeta> fragments = new ArrayList<>();
      GetResponse response =
          this.client
              .getKVClient()
              .get(
                  ByteSequence.from(FRAGMENT_NODE_PREFIX.getBytes()),
                  GetOption.newBuilder()
                      .withPrefix(ByteSequence.from(FRAGMENT_NODE_PREFIX.getBytes()))
                      .build())
              .get();
      for (KeyValue kv : response.getKvs()) {
        FragmentMeta fragment = JsonUtils.fromJson(kv.getValue().getBytes(), FragmentMeta.class);
        if (fragment.getKeyInterval().isIntersect(keyInterval)
            && fragment.getColumnsInterval().isContain(columnName)) {
          fragments.add(fragment);
        }
      }
      fragments.sort(
          (o1, o2) -> {
            long s1 = o1.getKeyInterval().getStartKey();
            long s2 = o2.getKeyInterval().getStartKey();
            return Long.compare(s2, s1);
          });
      return fragments;
    } catch (ExecutionException | InterruptedException e) {
      LOGGER.error("got error when get fragments by columnName and keyInterval: ", e);
    }
    return new ArrayList<>();
  }

  @Override
  public Map<ColumnsInterval, List<FragmentMeta>> getFragmentMapByColumnsIntervalAndKeyInterval(
      ColumnsInterval columnsInterval, KeyInterval keyInterval) {
    try {
      Map<ColumnsInterval, List<FragmentMeta>> fragmentsMap = new HashMap<>();
      GetResponse response =
          this.client
              .getKVClient()
              .get(
                  ByteSequence.from(FRAGMENT_NODE_PREFIX.getBytes()),
                  GetOption.newBuilder()
                      .withPrefix(ByteSequence.from(FRAGMENT_NODE_PREFIX.getBytes()))
                      .build())
              .get();
      for (KeyValue kv : response.getKvs()) {
        FragmentMeta fragment = JsonUtils.fromJson(kv.getValue().getBytes(), FragmentMeta.class);
        if (fragment.getKeyInterval().isIntersect(keyInterval)
            && fragment.getColumnsInterval().isIntersect(columnsInterval)) {
          fragmentsMap
              .computeIfAbsent(fragment.getColumnsInterval(), e -> new ArrayList<>())
              .add(fragment);
        }
      }
      fragmentsMap
          .values()
          .forEach(
              e ->
                  e.sort(
                      (o1, o2) -> {
                        long s1 = o1.getKeyInterval().getStartKey();
                        long s2 = o2.getKeyInterval().getStartKey();
                        return Long.compare(s1, s2);
                      }));
      return fragmentsMap;
    } catch (ExecutionException | InterruptedException e) {
      LOGGER.error("got error when get fragments by columnName and keyInterval: ", e);
    }
    return new HashMap<>();
  }

  @Override
  public void updateFragment(FragmentMeta fragmentMeta) throws MetaStorageException {
    try {
      client
          .getKVClient()
          .put(
              ByteSequence.from(
                  (FRAGMENT_NODE_PREFIX
                          + fragmentMeta.getColumnsInterval().toString()
                          + "/"
                          + fragmentMeta.getKeyInterval().toString())
                      .getBytes()),
              ByteSequence.from(JsonUtils.toJson(fragmentMeta)))
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new MetaStorageException("update storage unit error: ", e);
    }
  }

  @Override
  public void updateFragmentByColumnsInterval(
      ColumnsInterval columnsInterval, FragmentMeta fragmentMeta) throws MetaStorageException {
    try {
      client
          .getKVClient()
          .delete(
              ByteSequence.from(
                  (FRAGMENT_NODE_PREFIX
                          + columnsInterval.toString()
                          + "/"
                          + fragmentMeta.getKeyInterval().toString())
                      .getBytes()));
      GetResponse response =
          this.client
              .getKVClient()
              .get(
                  ByteSequence.from((FRAGMENT_NODE_PREFIX + columnsInterval.toString()).getBytes()),
                  GetOption.newBuilder()
                      .withPrefix(
                          ByteSequence.from(
                              (FRAGMENT_NODE_PREFIX + columnsInterval.toString()).getBytes()))
                      .build())
              .get();
      if (response.getKvs().isEmpty()) {
        client
            .getKVClient()
            .delete(
                ByteSequence.from((FRAGMENT_NODE_PREFIX + columnsInterval.toString()).getBytes()));
      }
      client
          .getKVClient()
          .put(
              ByteSequence.from(
                  (FRAGMENT_NODE_PREFIX
                          + fragmentMeta.getColumnsInterval().toString()
                          + "/"
                          + fragmentMeta.getKeyInterval().toString())
                      .getBytes()),
              ByteSequence.from(JsonUtils.toJson(fragmentMeta)))
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new MetaStorageException("update storage unit error: ", e);
    }
  }

  @Override
  public void removeFragment(FragmentMeta fragmentMeta) throws MetaStorageException {
    try {
      client
          .getKVClient()
          .delete(
              ByteSequence.from(
                  (FRAGMENT_NODE_PREFIX
                          + fragmentMeta.getColumnsInterval().toString()
                          + "/"
                          + fragmentMeta.getKeyInterval().toString())
                      .getBytes()));
      // 删除不需要的统计数据
      client
          .getKVClient()
          .delete(
              ByteSequence.from(
                  (STATISTICS_FRAGMENT_REQUESTS_PREFIX_WRITE
                          + "/"
                          + fragmentMeta.getColumnsInterval().toString()
                          + "/"
                          + fragmentMeta.getKeyInterval().toString())
                      .getBytes()));
      client
          .getKVClient()
          .delete(
              ByteSequence.from(
                  (STATISTICS_FRAGMENT_REQUESTS_PREFIX_READ
                          + "/"
                          + fragmentMeta.getColumnsInterval().toString()
                          + "/"
                          + fragmentMeta.getKeyInterval().toString())
                      .getBytes()));
      client
          .getKVClient()
          .delete(
              ByteSequence.from(
                  (STATISTICS_FRAGMENT_POINTS_PREFIX
                          + "/"
                          + fragmentMeta.getColumnsInterval().toString()
                          + "/"
                          + fragmentMeta.getKeyInterval().toString())
                      .getBytes()));
    } catch (Exception e) {
      throw new MetaStorageException("get error when remove fragment", e);
    }
  }

  @Override
  public void addFragment(FragmentMeta fragmentMeta) throws MetaStorageException {
    updateFragment(fragmentMeta);
  }

  @Override
  public void releaseFragment() throws MetaStorageException {
    try {
      client.getLockClient().unlock(ByteSequence.from(FRAGMENT_LOCK_NODE.getBytes())).get();
      client.getLeaseClient().revoke(fragmentLease).get();
      fragmentLease = -1L;
    } catch (Exception e) {
      throw new MetaStorageException("release fragment mutex error: ", e);
    } finally {
      fragmentLeaseLock.unlock();
    }
  }

  @Override
  public void registerFragmentChangeHook(FragmentChangeHook hook) {
    this.fragmentChangeHook = hook;
  }

  private void lockUser() throws MetaStorageException {
    try {
      userLeaseLock.lock();
      userLease = client.getLeaseClient().grant(MAX_LOCK_TIME).get().getID();
      client.getLockClient().lock(ByteSequence.from(USER_LOCK_NODE.getBytes()), userLease);
    } catch (Exception e) {
      userLeaseLock.unlock();
      throw new MetaStorageException("acquire user mutex error: ", e);
    }
  }

  private void releaseUser() throws MetaStorageException {
    try {
      client.getLockClient().unlock(ByteSequence.from(USER_LOCK_NODE.getBytes())).get();
      client.getLeaseClient().revoke(userLease).get();
      userLease = -1L;
    } catch (Exception e) {
      throw new MetaStorageException("release user mutex error: ", e);
    } finally {
      userLeaseLock.unlock();
    }
  }

  @Override
  public List<UserMeta> loadUser(UserMeta userMeta) throws MetaStorageException {
    Map<String, UserMeta> users = new HashMap<>();
    try {
      lockUser();
      GetResponse response =
          this.client
              .getKVClient()
              .get(
                  ByteSequence.from(USER_NODE_PREFIX.getBytes()),
                  GetOption.newBuilder()
                      .withPrefix(ByteSequence.from(USER_NODE_PREFIX.getBytes()))
                      .build())
              .get();
      if (response.getCount() != 0L) { // 服务器上已经有了，本地的不作数
        response
            .getKvs()
            .forEach(
                e -> {
                  UserMeta user = JsonUtils.fromJson(e.getValue().getBytes(), UserMeta.class);
                  users.put(user.getUsername(), user);
                });
        return new ArrayList<>(users.values());
      }
    } catch (ExecutionException | InterruptedException e) {
      LOGGER.error("got error when load user: ", e);
      throw new MetaStorageException(e);
    } finally {
      if (userLease != -1) {
        releaseUser();
      }
    }
    // 服务器上没有用户信息，添加本地用户信息
    addUser(userMeta);
    users.put(userMeta.getUsername(), userMeta);
    return new ArrayList<>(users.values());
  }

  @Override
  public void registerUserChangeHook(UserChangeHook hook) {
    userChangeHook = hook;
  }

  @Override
  public void addUser(UserMeta userMeta) throws MetaStorageException {
    updateUser(userMeta);
  }

  @Override
  public void updateUser(UserMeta userMeta) throws MetaStorageException {
    try {
      lockUser();
      this.client
          .getKVClient()
          .put(
              ByteSequence.from((USER_NODE_PREFIX + userMeta.getUsername()).getBytes()),
              ByteSequence.from(JsonUtils.toJson(userMeta)))
          .get();
    } catch (ExecutionException | InterruptedException e) {
      LOGGER.error("got error when add/update user: ", e);
      throw new MetaStorageException(e);
    } finally {
      if (userLease != -1) {
        releaseUser();
      }
    }
    if (userChangeHook != null) {
      userChangeHook.onChange(userMeta.getUsername(), userMeta);
    }
  }

  @Override
  public void removeUser(String username) throws MetaStorageException {
    try {
      lockUser();
      this.client
          .getKVClient()
          .delete(ByteSequence.from((USER_NODE_PREFIX + username).getBytes()))
          .get();
    } catch (ExecutionException | InterruptedException e) {
      LOGGER.error("got error when remove user: ", e);
      throw new MetaStorageException(e);
    } finally {
      if (userLease != -1) {
        releaseUser();
      }
    }
    if (userChangeHook != null) {
      userChangeHook.onChange(username, null);
    }
  }

  @Override
  public void registerTimeseriesChangeHook(TimeSeriesChangeHook hook) {}

  @Override
  public void registerVersionChangeHook(VersionChangeHook hook) {}

  @Override
  public boolean election() {
    return false;
  }

  @Override
  public void updateTimeseriesData(Map<String, Double> timeseriesData, long iginxid, long version)
      throws Exception {}

  @Override
  public Map<String, Double> getColumnsData() {
    return null;
  }

  @Override
  public void registerPolicy(long iginxId, int num) throws Exception {}

  @Override
  public int updateVersion() {
    return 0;
  }

  @Override
  public void updateTimeseriesLoad(Map<String, Long> timeseriesLoadMap) throws Exception {
    for (Map.Entry<String, Long> timeseriesLoadEntry : timeseriesLoadMap.entrySet()) {
      String path = STATISTICS_TIMESERIES_HEAT_PREFIX + "/" + timeseriesLoadEntry.getKey();
      if (this.client
              .getKVClient()
              .get(
                  ByteSequence.from(path.getBytes()),
                  GetOption.newBuilder().withPrefix(ByteSequence.from(path.getBytes())).build())
              .get()
          == null) {
        this.client
            .getKVClient()
            .put(
                ByteSequence.from(path.getBytes()),
                ByteSequence.from(JsonUtils.toJson(timeseriesLoadEntry.getValue())))
            .get();
      }
    }
    Map<String, Long> currentTimeseriesLoadMap = loadTimeseriesHeat();
    for (Map.Entry<String, Long> timeseriesLoadEntry : timeseriesLoadMap.entrySet()) {
      String path = STATISTICS_TIMESERIES_HEAT_PREFIX + "/" + timeseriesLoadEntry.getKey();
      this.client
          .getKVClient()
          .put(
              ByteSequence.from(path.getBytes()),
              ByteSequence.from(
                  JsonUtils.toJson(
                      timeseriesLoadEntry.getValue()
                          + currentTimeseriesLoadMap.getOrDefault(
                              timeseriesLoadEntry.getKey(), 0L))))
          .get();
    }
  }

  @Override
  public Map<String, Long> loadTimeseriesHeat() throws MetaStorageException, Exception {
    Map<String, Long> timeseriesHeatMap = new HashMap<>();
    GetResponse response =
        this.client
            .getKVClient()
            .get(
                ByteSequence.from(STATISTICS_TIMESERIES_HEAT_PREFIX.getBytes()),
                GetOption.newBuilder()
                    .withPrefix(ByteSequence.from(STATISTICS_TIMESERIES_HEAT_PREFIX.getBytes()))
                    .build())
            .get();
    for (KeyValue kv : response.getKvs()) {
      byte[] data = JsonUtils.fromJson(kv.getValue().getBytes(), byte[].class);
      long heat = JsonUtils.fromJson(data, Long.class);
      timeseriesHeatMap.put(kv.getKey().toString(), heat);
    }
    return timeseriesHeatMap;
  }

  @Override
  public void removeTimeseriesHeat() throws MetaStorageException {
    try {
      client.getKVClient().delete(ByteSequence.from(STATISTICS_TIMESERIES_HEAT_PREFIX.getBytes()));
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when removing timeseries heat: ", e);
    }
  }

  @Override
  public void lockTimeseriesHeatCounter() throws MetaStorageException {
    try {
      timeseriesHeatCounterLeaseLock.lock();
      timeseriesHeatCounterLease = client.getLeaseClient().grant(MAX_LOCK_TIME).get().getID();
      client
          .getLockClient()
          .lock(
              ByteSequence.from(TIMESERIES_HEAT_COUNTER_LOCK_NODE.getBytes()),
              timeseriesHeatCounterLease);
    } catch (Exception e) {
      timeseriesHeatCounterLeaseLock.unlock();
      throw new MetaStorageException("acquire fragment mutex error: ", e);
    }
  }

  @Override
  public void incrementTimeseriesHeatCounter() throws MetaStorageException {
    try {
      if (client
              .getKVClient()
              .get(ByteSequence.from(STATISTICS_TIMESERIES_HEAT_COUNTER_PREFIX.getBytes()))
              .get()
          == null) {
        client
            .getKVClient()
            .put(
                ByteSequence.from(STATISTICS_TIMESERIES_HEAT_COUNTER_PREFIX.getBytes()),
                ByteSequence.from(JsonUtils.toJson(1)))
            .get();
      } else {
        client
            .getKVClient()
            .put(
                ByteSequence.from(STATISTICS_TIMESERIES_HEAT_COUNTER_PREFIX.getBytes()),
                ByteSequence.from(JsonUtils.toJson(1 + getTimeseriesHeatCounter())))
            .get();
      }
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when updating timeseries heat counter: ", e);
    }
  }

  @Override
  public void resetTimeseriesHeatCounter() throws MetaStorageException {
    try {
      client
          .getKVClient()
          .put(
              ByteSequence.from(STATISTICS_TIMESERIES_HEAT_COUNTER_PREFIX.getBytes()),
              ByteSequence.from(JsonUtils.toJson(0)))
          .get();
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when resetting timeseries heat counter: ", e);
    }
  }

  @Override
  public void releaseTimeseriesHeatCounter() throws MetaStorageException {
    try {
      client
          .getLockClient()
          .unlock(ByteSequence.from(TIMESERIES_HEAT_COUNTER_LOCK_NODE.getBytes()))
          .get();
      client.getLeaseClient().revoke(timeseriesHeatCounterLease).get();
      timeseriesHeatCounterLease = -1L;
    } catch (Exception e) {
      throw new MetaStorageException("release fragment mutex error: ", e);
    } finally {
      timeseriesHeatCounterLeaseLock.unlock();
    }
  }

  @Override
  public int getTimeseriesHeatCounter() throws MetaStorageException {
    try {
      String[] tuples = STATISTICS_TIMESERIES_HEAT_COUNTER_PREFIX.split("/");
      String lastTuple = tuples[tuples.length - 1];
      StringBuilder newPrefix = new StringBuilder();
      for (int i = 0; i < tuples.length - 1; i++) {
        newPrefix.append(tuples[i]);
      }
      GetResponse response =
          client.getKVClient().get(ByteSequence.from(newPrefix.toString().getBytes())).get();
      if (!response.getKvs().isEmpty()) {
        for (KeyValue kv : response.getKvs()) {
          if (kv.getKey().toString().equals(lastTuple)) {
            byte[] data = JsonUtils.fromJson(kv.getValue().getBytes(), byte[].class);
            return JsonUtils.fromJson(data, Integer.class);
          }
        }
      }
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when get timeseries heat counter: ", e);
    }
    return 0;
  }

  @Override
  public void updateFragmentRequests(
      Map<FragmentMeta, Long> writeRequestsMap, Map<FragmentMeta, Long> readRequestsMap)
      throws Exception {
    for (Map.Entry<FragmentMeta, Long> writeRequestsEntry : writeRequestsMap.entrySet()) {
      if (writeRequestsEntry.getValue() > 0) {
        String requestsPath =
            STATISTICS_FRAGMENT_REQUESTS_PREFIX_WRITE
                + "/"
                + writeRequestsEntry.getKey().getColumnsInterval().toString()
                + "/"
                + writeRequestsEntry.getKey().getKeyInterval().toString();
        String pointsPath =
            STATISTICS_FRAGMENT_POINTS_PREFIX
                + "/"
                + writeRequestsEntry.getKey().getColumnsInterval().toString()
                + "/"
                + writeRequestsEntry.getKey().getKeyInterval().toString();
        GetResponse response =
            client.getKVClient().get(ByteSequence.from(requestsPath.getBytes())).get();
        if (response == null || response.getCount() <= 0) {
          client
              .getKVClient()
              .put(
                  ByteSequence.from(requestsPath.getBytes()),
                  ByteSequence.from(JsonUtils.toJson(writeRequestsEntry.getValue())));
        } else {
          long requests =
              JsonUtils.fromJson(response.getKvs().get(0).getValue().getBytes(), Long.class);
          client
              .getKVClient()
              .put(
                  ByteSequence.from(requestsPath.getBytes()),
                  ByteSequence.from(JsonUtils.toJson(requests + writeRequestsEntry.getValue())));
        }

        response = client.getKVClient().get(ByteSequence.from(pointsPath.getBytes())).get();
        if (response == null || response.getCount() <= 0) {
          client
              .getKVClient()
              .put(
                  ByteSequence.from(pointsPath.getBytes()),
                  ByteSequence.from(JsonUtils.toJson(writeRequestsEntry.getValue())));
        } else {
          long points =
              JsonUtils.fromJson(response.getKvs().get(0).getValue().getBytes(), Long.class);
          client
              .getKVClient()
              .put(
                  ByteSequence.from(pointsPath.getBytes()),
                  ByteSequence.from(JsonUtils.toJson(points + writeRequestsEntry.getValue())));
        }
      }
    }
    for (Map.Entry<FragmentMeta, Long> readRequestsEntry : readRequestsMap.entrySet()) {
      String path =
          STATISTICS_FRAGMENT_REQUESTS_PREFIX_READ
              + "/"
              + readRequestsEntry.getKey().getColumnsInterval().toString()
              + "/"
              + readRequestsEntry.getKey().getKeyInterval().toString();
      GetResponse response = client.getKVClient().get(ByteSequence.from(path.getBytes())).get();
      if (response == null || response.getCount() <= 0) {
        client
            .getKVClient()
            .put(
                ByteSequence.from(path.getBytes()),
                ByteSequence.from(JsonUtils.toJson(readRequestsEntry.getValue())));
      } else {
        long requests =
            JsonUtils.fromJson(response.getKvs().get(0).getValue().getBytes(), Long.class);
        client
            .getKVClient()
            .put(
                ByteSequence.from(path.getBytes()),
                ByteSequence.from(JsonUtils.toJson(requests + readRequestsEntry.getValue())));
      }
    }
  }

  @Override
  public void removeFragmentRequests() throws MetaStorageException {
    try {
      client
          .getKVClient()
          .delete(ByteSequence.from(STATISTICS_FRAGMENT_REQUESTS_PREFIX_WRITE.getBytes()))
          .get();
      client
          .getKVClient()
          .delete(ByteSequence.from(STATISTICS_FRAGMENT_REQUESTS_PREFIX_READ.getBytes()))
          .get();
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when removing fragment requests: ", e);
    }
  }

  @Override
  public void lockFragmentRequestsCounter() throws MetaStorageException {
    try {
      fragmentRequestsCounterLeaseLock.lock();
      fragmentRequestsCounterLease = client.getLeaseClient().grant(MAX_LOCK_TIME).get().getID();
      client
          .getLockClient()
          .lock(
              ByteSequence.from(TIMESERIES_HEAT_COUNTER_LOCK_NODE.getBytes()),
              fragmentRequestsCounterLease);
    } catch (Exception e) {
      fragmentRequestsCounterLeaseLock.unlock();
      throw new MetaStorageException(
          "encounter error when acquiring fragment requests counter mutex: ", e);
    }
  }

  @Override
  public void incrementFragmentRequestsCounter() throws MetaStorageException {
    try {
      client
          .getKVClient()
          .put(
              ByteSequence.from(STATISTICS_FRAGMENT_REQUESTS_COUNTER_PREFIX.getBytes()),
              ByteSequence.from(JsonUtils.toJson(1 + getFragmentRequestsCounter())))
          .get();
    } catch (Exception e) {
      throw new MetaStorageException(
          "encounter error when updating fragment requests counter: ", e);
    }
  }

  @Override
  public void resetFragmentRequestsCounter() throws MetaStorageException {
    try {
      client
          .getKVClient()
          .put(
              ByteSequence.from(STATISTICS_FRAGMENT_REQUESTS_COUNTER_PREFIX.getBytes()),
              ByteSequence.from(JsonUtils.toJson(0)));
    } catch (Exception e) {
      throw new MetaStorageException(
          "encounter error when resetting fragment requests counter: ", e);
    }
  }

  @Override
  public void releaseFragmentRequestsCounter() throws MetaStorageException {
    try {
      client
          .getLockClient()
          .unlock(ByteSequence.from(FRAGMENT_REQUESTS_COUNTER_LOCK_NODE.getBytes()))
          .get();
      client.getLeaseClient().revoke(timeseriesHeatCounterLease).get();
      fragmentRequestsCounterLease = -1L;
    } catch (Exception e) {
      throw new MetaStorageException(
          "encounter error when resetting fragment requests counter: ", e);
    } finally {
      fragmentRequestsCounterLeaseLock.unlock();
    }
  }

  @Override
  public int getFragmentRequestsCounter() throws MetaStorageException {
    try {
      String[] tuples = STATISTICS_FRAGMENT_REQUESTS_COUNTER_PREFIX.split("/");
      String lastTuple = tuples[tuples.length - 1];
      StringBuilder newPrefix = new StringBuilder();
      for (int i = 0; i < tuples.length - 1; i++) {
        newPrefix.append(tuples[i]);
      }
      GetResponse response =
          client.getKVClient().get(ByteSequence.from(newPrefix.toString().getBytes())).get();
      if (!response.getKvs().isEmpty()) {
        for (KeyValue kv : response.getKvs()) {
          if (kv.getKey().toString().equals(lastTuple)) {
            return JsonUtils.fromJson(kv.getValue().getBytes(), Integer.class);
          }
        }
      }
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when get fragment requests counter: ", e);
    }
    return 0;
  }

  @Override
  public Map<FragmentMeta, Long> loadFragmentPoints(IMetaCache cache) throws Exception {
    Map<FragmentMeta, Long> writePointsMap = new HashMap<>();
    GetResponse response =
        client
            .getKVClient()
            .get(ByteSequence.from(STATISTICS_FRAGMENT_POINTS_PREFIX.getBytes()))
            .get();
    Map<String, List<KeyValue>> timeSeriesRangeListMap = new HashMap<>();
    for (KeyValue kv : response.getKvs()) {
      String[] tuples = kv.getKey().toString().split("/");
      String timeSeriesRangeStr = tuples[tuples.length - 2];
      List<KeyValue> keyValues =
          timeSeriesRangeListMap.computeIfAbsent(timeSeriesRangeStr, k -> new ArrayList<>());
      keyValues.add(kv);
    }
    for (Map.Entry<String, List<KeyValue>> entry : timeSeriesRangeListMap.entrySet()) {
      ColumnsInterval columnsInterval = fromString(entry.getKey());
      List<FragmentMeta> fragmentMetas =
          cache.getFragmentMapByExactColumnsInterval(columnsInterval);
      for (KeyValue kv : entry.getValue()) {
        String[] tuples = kv.getKey().toString().split("/");
        long startTime = Long.parseLong(tuples[tuples.length - 1]);
        for (FragmentMeta fragmentMeta : fragmentMetas) {
          if (fragmentMeta.getKeyInterval().getStartKey() == startTime) {
            long points = JsonUtils.fromJson(kv.getValue().getBytes(), Long.class);
            writePointsMap.put(fragmentMeta, points);
          }
        }
      }
    }
    return writePointsMap;
  }

  @Override
  public void deleteFragmentPoints(ColumnsInterval columnsInterval, KeyInterval keyInterval)
      throws Exception {
    try {
      client
          .getKVClient()
          .delete(
              ByteSequence.from(
                  (STATISTICS_FRAGMENT_POINTS_PREFIX
                          + "/"
                          + columnsInterval.toString()
                          + "/"
                          + keyInterval.toString())
                      .getBytes()))
          .get();
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when removing fragment points: ", e);
    }
  }

  @Override
  public void updateFragmentPoints(FragmentMeta fragmentMeta, long points) throws Exception {
    String path =
        STATISTICS_FRAGMENT_POINTS_PREFIX
            + "/"
            + fragmentMeta.getColumnsInterval().toString()
            + "/"
            + fragmentMeta.getKeyInterval().toString();
    client
        .getKVClient()
        .put(ByteSequence.from(path.getBytes()), ByteSequence.from(JsonUtils.toJson(points)))
        .get();
  }

  @Override
  public void updateFragmentHeat(
      Map<FragmentMeta, Long> writeHotspotMap, Map<FragmentMeta, Long> readHotspotMap)
      throws Exception {
    for (Map.Entry<FragmentMeta, Long> writeHotspotEntry : writeHotspotMap.entrySet()) {
      String path =
          STATISTICS_FRAGMENT_HEAT_PREFIX_WRITE
              + "/"
              + writeHotspotEntry.getKey().getColumnsInterval().toString()
              + "/"
              + writeHotspotEntry.getKey().getKeyInterval().toString();
      GetResponse response = client.getKVClient().get(ByteSequence.from(path.getBytes())).get();
      if (response == null || response.getCount() <= 0) {
        client
            .getKVClient()
            .put(
                ByteSequence.from(path.getBytes()),
                ByteSequence.from(JsonUtils.toJson(writeHotspotEntry.getValue())));
      } else {
        long heat = JsonUtils.fromJson(response.getKvs().get(0).getValue().getBytes(), Long.class);
        client
            .getKVClient()
            .put(
                ByteSequence.from(path.getBytes()),
                ByteSequence.from(JsonUtils.toJson(heat + writeHotspotEntry.getValue())));
      }
    }
    for (Map.Entry<FragmentMeta, Long> readHotspotEntry : readHotspotMap.entrySet()) {
      String path =
          STATISTICS_FRAGMENT_HEAT_PREFIX_READ
              + "/"
              + readHotspotEntry.getKey().getColumnsInterval().toString()
              + "/"
              + readHotspotEntry.getKey().getKeyInterval().toString();
      GetResponse response = client.getKVClient().get(ByteSequence.from(path.getBytes())).get();
      if (response == null || response.getCount() <= 0) {
        client
            .getKVClient()
            .put(
                ByteSequence.from(path.getBytes()),
                ByteSequence.from(JsonUtils.toJson(readHotspotEntry.getValue())));
      } else {
        long heat = JsonUtils.fromJson(response.getKvs().get(0).getValue().getBytes(), Long.class);
        client
            .getKVClient()
            .put(
                ByteSequence.from(path.getBytes()),
                ByteSequence.from(JsonUtils.toJson(heat + readHotspotEntry.getValue())));
      }
    }
  }

  @Override
  public Pair<Map<FragmentMeta, Long>, Map<FragmentMeta, Long>> loadFragmentHeat(IMetaCache cache)
      throws Exception {
    Map<FragmentMeta, Long> writeHotspotMap = new HashMap<>();
    Map<FragmentMeta, Long> readHotspotMap = new HashMap<>();
    GetResponse writeResponse =
        client
            .getKVClient()
            .get(ByteSequence.from(STATISTICS_FRAGMENT_HEAT_PREFIX_WRITE.getBytes()))
            .get();
    GetResponse readResponse =
        client
            .getKVClient()
            .get(ByteSequence.from(STATISTICS_FRAGMENT_HEAT_PREFIX_READ.getBytes()))
            .get();
    Map<String, List<KeyValue>> timeSeriesWriteRangeListMap = new HashMap<>();
    Map<String, List<KeyValue>> timeSeriesReadRangeListMap = new HashMap<>();
    if (writeResponse != null) {
      for (KeyValue kv : writeResponse.getKvs()) {
        String[] tuples = kv.getKey().toString().split("/");
        List<KeyValue> keyValues =
            timeSeriesWriteRangeListMap.computeIfAbsent(
                tuples[tuples.length - 2], k -> new ArrayList<>());
        keyValues.add(kv);
      }
    }
    if (readResponse != null) {
      for (KeyValue kv : readResponse.getKvs()) {
        String[] tuples = kv.getKey().toString().split("/");
        List<KeyValue> keyValues =
            timeSeriesReadRangeListMap.computeIfAbsent(
                tuples[tuples.length - 2], k -> new ArrayList<>());
        keyValues.add(kv);
      }
    }
    for (Map.Entry<String, List<KeyValue>> entry : timeSeriesWriteRangeListMap.entrySet()) {
      ColumnsInterval columnsInterval = fromString(entry.getKey());
      Map<ColumnsInterval, List<FragmentMeta>> fragmentMapOfTimeSeriesInterval =
          cache.getFragmentMapByColumnsInterval(columnsInterval);
      List<FragmentMeta> fragmentMetas = fragmentMapOfTimeSeriesInterval.get(columnsInterval);

      if (fragmentMetas != null) {
        for (KeyValue kv : entry.getValue()) {
          String[] tuples = kv.getKey().toString().split("/");
          long startTime = Long.parseLong(tuples[tuples.length - 1]);
          for (FragmentMeta fragmentMeta : fragmentMetas) {
            if (fragmentMeta.getKeyInterval().getStartKey() == startTime) {
              long heat = JsonUtils.fromJson(kv.getValue().getBytes(), Long.class);
              writeHotspotMap.put(fragmentMeta, heat);
            }
          }
        }
      }
    }
    for (Map.Entry<String, List<KeyValue>> entry : timeSeriesReadRangeListMap.entrySet()) {
      ColumnsInterval columnsInterval = fromString(entry.getKey());
      Map<ColumnsInterval, List<FragmentMeta>> fragmentMapOfTimeSeriesInterval =
          cache.getFragmentMapByColumnsInterval(columnsInterval);
      List<FragmentMeta> fragmentMetas = fragmentMapOfTimeSeriesInterval.get(columnsInterval);

      if (fragmentMetas != null) {
        for (KeyValue kv : entry.getValue()) {
          String[] tuples = kv.getKey().toString().split("/");
          long startTime = Long.parseLong(tuples[tuples.length - 1]);
          for (FragmentMeta fragmentMeta : fragmentMetas) {
            if (fragmentMeta.getKeyInterval().getStartKey() == startTime) {
              long heat = JsonUtils.fromJson(kv.getValue().getBytes(), Long.class);
              readHotspotMap.put(fragmentMeta, heat);
            }
          }
        }
      }
    }
    return new Pair<>(writeHotspotMap, readHotspotMap);
  }

  @Override
  public void removeFragmentHeat() throws MetaStorageException {
    try {
      client
          .getKVClient()
          .delete(ByteSequence.from(STATISTICS_FRAGMENT_HEAT_PREFIX_WRITE.getBytes()))
          .get();
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when removing fragment heat: ", e);
    }
  }

  @Override
  public void lockFragmentHeatCounter() throws MetaStorageException {
    try {
      fragmentHeatCounterLeaseLock.lock();
      fragmentHeatCounterLease = client.getLeaseClient().grant(MAX_LOCK_TIME).get().getID();
      client
          .getLockClient()
          .lock(
              ByteSequence.from(FRAGMENT_HEAT_COUNTER_LOCK_NODE.getBytes()),
              fragmentHeatCounterLease);
    } catch (Exception e) {
      fragmentHeatCounterLeaseLock.unlock();
      throw new MetaStorageException("acquire fragment heat counter mutex error: ", e);
    }
  }

  @Override
  public void incrementFragmentHeatCounter() throws MetaStorageException {
    client
        .getKVClient()
        .put(
            ByteSequence.from(STATISTICS_FRAGMENT_HEAT_COUNTER_PREFIX.getBytes()),
            ByteSequence.from(JsonUtils.toJson(getFragmentHeatCounter() + 1)));
  }

  @Override
  public void resetFragmentHeatCounter() throws MetaStorageException {
    client
        .getKVClient()
        .put(
            ByteSequence.from(STATISTICS_FRAGMENT_HEAT_COUNTER_PREFIX.getBytes()),
            ByteSequence.from(JsonUtils.toJson(0)));
  }

  @Override
  public void releaseFragmentHeatCounter() throws MetaStorageException {
    try {
      client
          .getLockClient()
          .unlock(ByteSequence.from(FRAGMENT_HEAT_COUNTER_LOCK_NODE.getBytes()))
          .get();
      client.getLeaseClient().revoke(fragmentHeatCounterLease).get();
      fragmentHeatCounterLease = -1L;
    } catch (Exception e) {
      throw new MetaStorageException("release fragment heat counter mutex error: ", e);
    } finally {
      fragmentHeatCounterLeaseLock.unlock();
    }
  }

  @Override
  public int getFragmentHeatCounter() throws MetaStorageException {
    try {
      String[] tuples = STATISTICS_FRAGMENT_HEAT_COUNTER_PREFIX.split("/");
      String lastTuple = tuples[tuples.length - 1];
      StringBuilder newPrefix = new StringBuilder();
      for (int i = 0; i < tuples.length - 1; i++) {
        newPrefix.append(tuples[i]);
      }
      GetResponse response =
          client.getKVClient().get(ByteSequence.from(newPrefix.toString().getBytes())).get();
      if (!response.getKvs().isEmpty()) {
        for (KeyValue kv : response.getKvs()) {
          if (kv.getKey().toString().equals(lastTuple)) {
            return JsonUtils.fromJson(kv.getValue().getBytes(), Integer.class);
          }
        }
      }
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when get fragment heat counter: ", e);
    }
    return 0;
  }

  @Override
  public boolean proposeToReshard() throws MetaStorageException {
    ReshardStatus currStatus = getReshardStatus();
    if (currStatus == null || (currStatus.equals(NON_RESHARDING) || currStatus.equals(JUDGING))) {
      updateReshardStatus(EXECUTING);
      return true;
    }
    return false;
  }

  private ReshardStatus getReshardStatus() throws MetaStorageException {
    try {
      String[] tuples = RESHARD_STATUS_NODE_PREFIX.split("/");
      String lastTuple = tuples[tuples.length - 1];
      StringBuilder newPrefix = new StringBuilder();
      for (int i = 0; i < tuples.length - 1; i++) {
        newPrefix.append(tuples[i]);
      }
      GetResponse response =
          client.getKVClient().get(ByteSequence.from(newPrefix.toString().getBytes())).get();
      if (!response.getKvs().isEmpty()) {
        for (KeyValue kv : response.getKvs()) {
          if (kv.getKey().toString().equals(lastTuple)) {
            return JsonUtils.fromJson(kv.getValue().getBytes(), ReshardStatus.class);
          }
        }
      }
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when get reshard status: ", e);
    }
    return null;
  }

  @Override
  public void lockReshardStatus() throws MetaStorageException {
    try {
      reshardStatusLeaseLock.lock();
      reshardStatusLease = client.getLeaseClient().grant(MAX_LOCK_TIME).get().getID();
      client
          .getLockClient()
          .lock(ByteSequence.from(RESHARD_STATUS_LOCK_NODE.getBytes()), reshardStatusLease);
    } catch (Exception e) {
      reshardStatusLeaseLock.unlock();
      throw new MetaStorageException("acquire reshard status mutex error: ", e);
    }
  }

  @Override
  public void updateReshardStatus(ReshardStatus status) throws MetaStorageException {
    try {
      client
          .getKVClient()
          .put(
              ByteSequence.from(RESHARD_STATUS_NODE_PREFIX.getBytes()),
              ByteSequence.from(JsonUtils.toJson(status)))
          .get();
    } catch (Exception e) {
      throw new MetaStorageException("update reshard status mutex error: ", e);
    }
  }

  @Override
  public void releaseReshardStatus() throws MetaStorageException {
    try {
      client.getLockClient().unlock(ByteSequence.from(RESHARD_STATUS_LOCK_NODE.getBytes())).get();
      client.getLeaseClient().revoke(reshardStatusLease).get();
      reshardStatusLease = -1L;
    } catch (Exception e) {
      throw new MetaStorageException("release reshard status mutex error: ", e);
    } finally {
      reshardStatusLeaseLock.unlock();
    }
  }

  @Override
  public void removeReshardStatus() throws MetaStorageException {
    try {
      client.getKVClient().delete(ByteSequence.from(RESHARD_STATUS_NODE_PREFIX.getBytes())).get();
    } catch (Exception e) {
      throw new MetaStorageException("remove reshard status mutex error: ", e);
    }
  }

  @Override
  public void registerReshardStatusHook(ReshardStatusChangeHook hook) {
    this.reshardStatusChangeHook = hook;
  }

  @Override
  public void lockReshardCounter() throws MetaStorageException {
    try {
      reshardCounterLeaseLock.lock();
      reshardCounterLease = client.getLeaseClient().grant(MAX_LOCK_TIME).get().getID();
      client
          .getLockClient()
          .lock(ByteSequence.from(RESHARD_COUNTER_LOCK_NODE.getBytes()), reshardCounterLease);
    } catch (Exception e) {
      reshardCounterLeaseLock.unlock();
      throw new MetaStorageException("acquire reshard counter mutex error: ", e);
    }
  }

  @Override
  public void incrementReshardCounter() throws MetaStorageException {
    client
        .getKVClient()
        .put(
            ByteSequence.from(RESHARD_COUNTER_NODE_PREFIX.getBytes()),
            ByteSequence.from(JsonUtils.toJson(getReshardCounter() + 1)));
  }

  @Override
  public void resetReshardCounter() throws MetaStorageException {
    client
        .getKVClient()
        .put(
            ByteSequence.from(RESHARD_COUNTER_NODE_PREFIX.getBytes()),
            ByteSequence.from(JsonUtils.toJson(0)));
  }

  private int getReshardCounter() throws MetaStorageException {
    try {
      String[] tuples = RESHARD_COUNTER_NODE_PREFIX.split("/");
      String lastTuple = tuples[tuples.length - 1];
      StringBuilder newPrefix = new StringBuilder();
      for (int i = 0; i < tuples.length - 1; i++) {
        newPrefix.append(tuples[i]);
      }
      GetResponse response =
          client.getKVClient().get(ByteSequence.from(newPrefix.toString().getBytes())).get();
      if (!response.getKvs().isEmpty()) {
        for (KeyValue kv : response.getKvs()) {
          if (kv.getKey().toString().equals(lastTuple)) {
            return JsonUtils.fromJson(kv.getValue().getBytes(), Integer.class);
          }
        }
      }
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when get reshard counter: ", e);
    }
    return 0;
  }

  @Override
  public void releaseReshardCounter() throws MetaStorageException {
    try {
      client.getLockClient().unlock(ByteSequence.from(RESHARD_COUNTER_LOCK_NODE.getBytes())).get();
      client.getLeaseClient().revoke(reshardCounterLease).get();
      reshardCounterLease = -1L;
    } catch (Exception e) {
      throw new MetaStorageException("release reshard counter mutex error: ", e);
    } finally {
      reshardCounterLeaseLock.unlock();
    }
  }

  @Override
  public void removeReshardCounter() throws MetaStorageException {
    try {
      client.getKVClient().delete(ByteSequence.from(RESHARD_COUNTER_NODE_PREFIX.getBytes())).get();
    } catch (Exception e) {
      throw new MetaStorageException("remove reshard counter mutex error: ", e);
    }
  }

  @Override
  public void registerReshardCounterChangeHook(ReshardCounterChangeHook hook) {
    this.reshardCounterChangeHook = hook;
  }

  private void lockTransform() throws MetaStorageException {
    try {
      transformLeaseLock.lock();
      transformLease = client.getLeaseClient().grant(MAX_LOCK_TIME).get().getID();
      client
          .getLockClient()
          .lock(ByteSequence.from(TRANSFORM_LOCK_NODE.getBytes()), transformLease);
    } catch (Exception e) {
      transformLeaseLock.unlock();
      throw new MetaStorageException("acquire transform mutex error: ", e);
    }
  }

  private void releaseTransform() throws MetaStorageException {
    try {
      client.getLockClient().unlock(ByteSequence.from(TRANSFORM_LOCK_NODE.getBytes())).get();
      client.getLeaseClient().revoke(transformLease).get();
      transformLease = -1L;
    } catch (Exception e) {
      throw new MetaStorageException("release user mutex error: ", e);
    } finally {
      transformLeaseLock.unlock();
    }
  }

  @Override
  public void registerTransformChangeHook(TransformChangeHook hook) {
    transformChangeHook = hook;
  }

  @Override
  public List<TransformTaskMeta> loadTransformTask() throws MetaStorageException {
    try {
      lockTransform();
      Map<String, TransformTaskMeta> taskMetaMap = new HashMap<>();
      GetResponse response =
          this.client
              .getKVClient()
              .get(
                  ByteSequence.from(TRANSFORM_NODE_PREFIX.getBytes()),
                  GetOption.newBuilder()
                      .withPrefix(ByteSequence.from(TRANSFORM_NODE_PREFIX.getBytes()))
                      .build())
              .get();
      if (response.getCount() != 0L) {
        response
            .getKvs()
            .forEach(
                e -> {
                  TransformTaskMeta taskMeta =
                      JsonUtils.fromJson(e.getValue().getBytes(), TransformTaskMeta.class);
                  taskMetaMap.put(taskMeta.getName(), taskMeta);
                });
      }
      return new ArrayList<>(taskMetaMap.values());
    } catch (ExecutionException | InterruptedException e) {
      LOGGER.error("got error when load transform: ", e);
      throw new MetaStorageException(e);
    } finally {
      if (transformLease != -1) {
        releaseTransform();
      }
    }
  }

  @Override
  public void addTransformTask(TransformTaskMeta transformTask) throws MetaStorageException {
    updateTransformTask(transformTask);
  }

  @Override
  public void updateTransformTask(TransformTaskMeta transformTask) throws MetaStorageException {
    try {
      lockTransform();
      this.client
          .getKVClient()
          .put(
              ByteSequence.from((TRANSFORM_NODE_PREFIX + transformTask.getName()).getBytes()),
              ByteSequence.from(JsonUtils.toJson(transformTask)))
          .get();
    } catch (ExecutionException | InterruptedException e) {
      LOGGER.error("got error when add/update transform: ", e);
      throw new MetaStorageException(e);
    } finally {
      if (transformLease != -1) {
        releaseTransform();
      }
    }
    if (transformChangeHook != null) {
      transformChangeHook.onChange(transformTask.getName(), transformTask);
    }
  }

  @Override
  public void dropTransformTask(String name) throws MetaStorageException {
    try {
      lockTransform();
      this.client
          .getKVClient()
          .delete(ByteSequence.from((TRANSFORM_NODE_PREFIX + name).getBytes()))
          .get();
    } catch (ExecutionException | InterruptedException e) {
      LOGGER.error("got error when remove transform: ", e);
      throw new MetaStorageException(e);
    } finally {
      if (transformLease != -1) {
        releaseTransform();
      }
    }
    if (transformChangeHook != null) {
      transformChangeHook.onChange(name, null);
    }
  }

  @Override
  public List<TriggerDescriptor> loadJobTrigger() throws MetaStorageException {
    try {
      lockJobTrigger();
      Map<String, TriggerDescriptor> triggerMap = new HashMap<>();
      GetResponse response =
          this.client
              .getKVClient()
              .get(
                  ByteSequence.from(JOB_TRIGGER_NODE_PREFIX.getBytes()),
                  GetOption.newBuilder()
                      .withPrefix(ByteSequence.from(JOB_TRIGGER_NODE_PREFIX.getBytes()))
                      .build())
              .get();
      if (response.getCount() != 0L) {
        response
            .getKvs()
            .forEach(
                e -> {
                  TriggerDescriptor descriptor =
                      JsonUtils.fromJson(e.getValue().getBytes(), TriggerDescriptor.class);
                  triggerMap.put(descriptor.getName(), descriptor);
                });
      }
      return new ArrayList<>(triggerMap.values());
    } catch (ExecutionException | InterruptedException e) {
      LOGGER.error("got error when load job triggers: ", e);
      throw new MetaStorageException(e);
    } finally {
      if (jobTriggerLease != -1) {
        releaseJobTrigger();
      }
    }
  }

  @Override
  public void registerJobTriggerChangeHook(JobTriggerChangeHook hook) {
    jobTriggerChangeHook = hook;
  }

  private void lockJobTrigger() throws MetaStorageException {
    try {
      jobTriggerLeaseLock.lock();
      jobTriggerLease = client.getLeaseClient().grant(MAX_LOCK_TIME).get().getID();
      client
          .getLockClient()
          .lock(ByteSequence.from(JOB_TRIGGER_LOCK_NODE.getBytes()), jobTriggerLease);
    } catch (Exception e) {
      jobTriggerLeaseLock.unlock();
      throw new MetaStorageException("acquire job trigger mutex error: ", e);
    }
  }

  private void releaseJobTrigger() throws MetaStorageException {
    try {
      client.getLockClient().unlock(ByteSequence.from(JOB_TRIGGER_LOCK_NODE.getBytes())).get();
      client.getLeaseClient().revoke(jobTriggerLease).get();
      jobTriggerLease = -1L;
    } catch (Exception e) {
      throw new MetaStorageException("release job trigger mutex error: ", e);
    } finally {
      jobTriggerLeaseLock.unlock();
    }
  }

  @Override
  public void storeJobTrigger(TriggerDescriptor jobTriggerDescriptor) throws MetaStorageException {
    updateJobTrigger(jobTriggerDescriptor);
  }

  @Override
  public void updateJobTrigger(TriggerDescriptor descriptor) throws MetaStorageException {
    try {
      lockJobTrigger();
      this.client
          .getKVClient()
          .put(
              ByteSequence.from((JOB_TRIGGER_NODE_PREFIX + descriptor.getName()).getBytes()),
              ByteSequence.from(JsonUtils.toJson(descriptor)))
          .get();
    } catch (ExecutionException | InterruptedException e) {
      LOGGER.error("got error when storing job trigger: ", e);
      throw new MetaStorageException(e);
    } finally {
      if (jobTriggerLease != -1) {
        releaseJobTrigger();
      }
    }
    if (jobTriggerChangeHook != null) {
      jobTriggerChangeHook.onChange(descriptor.getName(), descriptor);
    }
  }

  @Override
  public void dropJobTrigger(String name) throws MetaStorageException {
    try {
      lockJobTrigger();
      this.client
          .getKVClient()
          .delete(ByteSequence.from((JOB_TRIGGER_NODE_PREFIX + name).getBytes()))
          .get();
    } catch (ExecutionException | InterruptedException e) {
      LOGGER.error("got error when remove job trigger: ", e);
      throw new MetaStorageException(e);
    } finally {
      if (jobTriggerLease != -1) {
        releaseJobTrigger();
      }
    }
    if (jobTriggerChangeHook != null) {
      jobTriggerChangeHook.onChange(name, null);
    }
  }

  @Override
  public void lockMaxActiveEndKeyStatistics() throws MetaStorageException {
    try {
      maxActiveEndTimeStatisticsLeaseLock.lock();
      maxActiveEndTimeStatisticsLease = client.getLeaseClient().grant(MAX_LOCK_TIME).get().getID();
      client
          .getLockClient()
          .lock(
              ByteSequence.from(ACTIVE_END_TIME_COUNTER_LOCK_NODE.getBytes()),
              maxActiveEndTimeStatisticsLease);
    } catch (Exception e) {
      maxActiveEndTimeStatisticsLeaseLock.unlock();
      throw new MetaStorageException("acquire max active end time mutex error: ", e);
    }
  }

  @Override
  public void addOrUpdateMaxActiveEndKeyStatistics(long endKey) throws MetaStorageException {
    try {
      client
          .getKVClient()
          .put(
              ByteSequence.from(MAX_ACTIVE_END_TIME_STATISTICS_NODE.getBytes()),
              ByteSequence.from(JsonUtils.toJson(endKey)));
    } catch (Exception e) {
      throw new MetaStorageException(
          "encounter error when adding or updating max active end time statistics: ", e);
    }
  }

  @Override
  public long getMaxActiveEndKeyStatistics() throws MetaStorageException {
    try {
      String[] tuples = MAX_ACTIVE_END_TIME_STATISTICS_NODE.split("/");
      String lastTuple = tuples[tuples.length - 1];
      StringBuilder newPrefix = new StringBuilder();
      for (int i = 0; i < tuples.length - 1; i++) {
        newPrefix.append(tuples[i]);
      }
      GetResponse response =
          client.getKVClient().get(ByteSequence.from(newPrefix.toString().getBytes())).get();
      if (!response.getKvs().isEmpty()) {
        for (KeyValue kv : response.getKvs()) {
          if (kv.getKey().toString().equals(lastTuple)) {
            return JsonUtils.fromJson(kv.getValue().getBytes(), Integer.class);
          }
        }
      }
    } catch (Exception e) {
      throw new MetaStorageException("encounter error when get max active end time: ", e);
    }
    return 0;
  }

  @Override
  public void releaseMaxActiveEndKeyStatistics() throws MetaStorageException {
    try {
      client
          .getLockClient()
          .unlock(ByteSequence.from(ACTIVE_END_TIME_COUNTER_LOCK_NODE.getBytes()))
          .get();
      client.getLeaseClient().revoke(maxActiveEndTimeStatisticsLease).get();
      maxActiveEndTimeStatisticsLease = -1L;
    } catch (Exception e) {
      throw new MetaStorageException("release user mutex error: ", e);
    } finally {
      maxActiveEndTimeStatisticsLeaseLock.unlock();
    }
  }

  @Override
  public void registerMaxActiveEndKeyStatisticsChangeHook(MaxActiveEndKeyStatisticsChangeHook hook)
      throws MetaStorageException {
    this.maxActiveEndKeyStatisticsChangeHook = hook;
  }

  public void close() throws MetaStorageException {
    this.iginxWatcher.close();
    this.iginxWatcher = null;

    this.storageWatcher.close();
    this.storageWatcher = null;

    this.storageUnitWatcher.close();
    this.storageUnitWatcher = null;

    this.fragmentWatcher.close();
    this.fragmentWatcher = null;

    this.userWatcher.close();
    this.userWatcher = null;

    this.transformWatcher.close();
    this.transformWatcher = null;

    this.client.close();
    this.client = null;
  }
}
