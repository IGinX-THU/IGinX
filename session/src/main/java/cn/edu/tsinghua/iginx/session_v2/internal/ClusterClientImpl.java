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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.session_v2.internal;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session_v2.Arguments;
import cn.edu.tsinghua.iginx.session_v2.ClusterClient;
import cn.edu.tsinghua.iginx.session_v2.domain.ClusterInfo;
import cn.edu.tsinghua.iginx.session_v2.domain.Storage;
import cn.edu.tsinghua.iginx.session_v2.exception.IginXException;
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
  public ClusterInfo getClusterInfo() throws IginXException {
    GetClusterInfoReq req = new GetClusterInfoReq(sessionId);

    GetClusterInfoResp resp;
    synchronized (iginXClient) {
      iginXClient.checkIsClosed();

      try {
        resp = client.getClusterInfo(req);
        StatusUtils.verifySuccess(resp.status);
      } catch (TException | SessionException e) {
        throw new IginXException("get cluster info failure: ", e);
      }
    }

    return new ClusterInfo(
        resp.getIginxInfos(),
        resp.getStorageEngineInfos(),
        resp.getLocalMetaStorageInfo(),
        resp.getMetaStorageInfos());
  }

  @Override
  public void scaleOutStorage(Storage storage) throws IginXException {
    Arguments.checkNotNull(storage, "storage");
    scaleOutStorages(Collections.singletonList(storage));
  }

  @Override
  public void scaleOutStorages(List<Storage> storages) throws IginXException {
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
        throw new IginXException("scale out storage failure: ", e);
      }
    }
  }

  @Override
  public int getReplicaNum() throws IginXException {
    GetReplicaNumReq req = new GetReplicaNumReq(sessionId);

    GetReplicaNumResp resp;
    synchronized (iginXClient) {
      iginXClient.checkIsClosed();
      try {
        resp = client.getReplicaNum(req);
        StatusUtils.verifySuccess(resp.status);
      } catch (TException | SessionException e) {
        throw new IginXException("get replica num failure: ", e);
      }
    }
    return resp.getReplicaNum();
  }

  private static StorageEngine toStorageEngine(Storage storage) {
    return new StorageEngine(
        storage.getIp(), storage.getPort(), storage.getType(), storage.getExtraParams());
  }
}
