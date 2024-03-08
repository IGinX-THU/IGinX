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
package cn.edu.tsinghua.iginx.session_v2.internal;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session_v2.Arguments;
import cn.edu.tsinghua.iginx.session_v2.ClusterClient;
import cn.edu.tsinghua.iginx.session_v2.domain.ClusterInfo;
import cn.edu.tsinghua.iginx.session_v2.domain.Storage;
import cn.edu.tsinghua.iginx.thrift.AddStorageEnginesReq;
import cn.edu.tsinghua.iginx.thrift.GetClusterInfoReq;
import cn.edu.tsinghua.iginx.thrift.GetClusterInfoResp;
import cn.edu.tsinghua.iginx.thrift.GetReplicaNumReq;
import cn.edu.tsinghua.iginx.thrift.GetReplicaNumResp;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.thrift.StorageEngine;
import cn.edu.tsinghua.iginx.utils.StatusUtils;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.thrift.TException;

public class ClusterClientImpl extends AbstractFunctionClient implements ClusterClient {

  public ClusterClientImpl(IginXClientImpl iginXClient) {
    super(iginXClient);
  }

  @Override
  public ClusterInfo getClusterInfo() {
    GetClusterInfoReq req = new GetClusterInfoReq(sessionId);

    GetClusterInfoResp resp;
    synchronized (iginXClient) {
      iginXClient.checkIsClosed();

      try {
        resp = client.getClusterInfo(req);
        StatusUtils.verifySuccess(resp.status);
      } catch (TException | SessionException e) {
        throw new RuntimeException("get cluster info failure: ", e);
      }
    }

    return new ClusterInfo(
        resp.getIginxInfos(),
        resp.getStorageEngineInfos(),
        resp.getLocalMetaStorageInfo(),
        resp.getMetaStorageInfos());
  }

  @Override
  public void scaleOutStorage(Storage storage) {
    Arguments.checkNotNull(storage, "storage");
    scaleOutStorages(Collections.singletonList(storage));
  }

  @Override
  public void scaleOutStorages(List<Storage> storages) {
    Arguments.checkNotNull(storages, "storages");
    storages.forEach(storage -> Arguments.checkNotNull(storage, "storage"));

    List<StorageEngine> storageEngines =
        storages.stream().map(ClusterClientImpl::toStorageEngine).collect(Collectors.toList());

    AddStorageEnginesReq req = new AddStorageEnginesReq(sessionId, storageEngines);

    synchronized (iginXClient) {
      iginXClient.checkIsClosed();
      try {
        Status status = client.addStorageEngines(req);
        StatusUtils.verifySuccess(status);
      } catch (TException | SessionException e) {
        throw new RuntimeException("scale out storage failure: ", e);
      }
    }
  }

  @Override
  public int getReplicaNum() {
    GetReplicaNumReq req = new GetReplicaNumReq(sessionId);

    GetReplicaNumResp resp;
    synchronized (iginXClient) {
      iginXClient.checkIsClosed();
      try {
        resp = client.getReplicaNum(req);
        StatusUtils.verifySuccess(resp.status);
      } catch (TException | SessionException e) {
        throw new RuntimeException("get replica num failure: ", e);
      }
    }
    return resp.getReplicaNum();
  }

  private static StorageEngine toStorageEngine(Storage storage) {
    return new StorageEngine(
        storage.getIp(), storage.getPort(), storage.getType(), storage.getExtraParams());
  }
}
