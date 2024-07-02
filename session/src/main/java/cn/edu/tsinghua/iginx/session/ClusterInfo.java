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
package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.thrift.GetClusterInfoResp;
import cn.edu.tsinghua.iginx.thrift.IginxInfo;
import cn.edu.tsinghua.iginx.thrift.LocalMetaStorageInfo;
import cn.edu.tsinghua.iginx.thrift.MetaStorageInfo;
import cn.edu.tsinghua.iginx.thrift.StorageEngineInfo;
import java.util.List;

public class ClusterInfo {

  private final GetClusterInfoResp resp;

  public ClusterInfo(GetClusterInfoResp resp) {
    this.resp = resp;
  }

  public List<IginxInfo> getIginxInfos() {
    return resp.getIginxInfos();
  }

  public List<StorageEngineInfo> getStorageEngineInfos() {
    return resp.getStorageEngineInfos();
  }

  public boolean isUseLocalMetaStorage() {
    return resp.isSetLocalMetaStorageInfo();
  }

  public LocalMetaStorageInfo getLocalMetaStorageInfo() {
    return resp.getLocalMetaStorageInfo();
  }

  public List<MetaStorageInfo> getMetaStorageInfos() {
    return resp.getMetaStorageInfos();
  }
}
