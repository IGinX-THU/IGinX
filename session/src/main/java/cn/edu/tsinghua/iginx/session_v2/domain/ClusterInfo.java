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
package cn.edu.tsinghua.iginx.session_v2.domain;

import cn.edu.tsinghua.iginx.thrift.IginxInfo;
import cn.edu.tsinghua.iginx.thrift.LocalMetaStorageInfo;
import cn.edu.tsinghua.iginx.thrift.MetaStorageInfo;
import cn.edu.tsinghua.iginx.thrift.StorageEngineInfo;
import java.util.List;

public final class ClusterInfo {

  private final List<IginxInfo> iginxInfos;

  private final List<StorageEngineInfo> storageEngineInfos;

  private final LocalMetaStorageInfo localMetaStorageInfo;

  private final List<MetaStorageInfo> metaStorageInfos;

  public ClusterInfo(
      List<IginxInfo> iginxInfos,
      List<StorageEngineInfo> storageEngineInfos,
      LocalMetaStorageInfo localMetaStorageInfo,
      List<MetaStorageInfo> metaStorageInfos) {
    this.iginxInfos = iginxInfos;
    this.storageEngineInfos = storageEngineInfos;
    this.localMetaStorageInfo = localMetaStorageInfo;
    this.metaStorageInfos = metaStorageInfos;
  }

  public ClusterInfo(
      List<IginxInfo> iginxInfos,
      List<StorageEngineInfo> storageEngineInfos,
      LocalMetaStorageInfo localMetaStorageInfo) {
    this(iginxInfos, storageEngineInfos, localMetaStorageInfo, null);
  }

  public ClusterInfo(
      List<IginxInfo> iginxInfos,
      List<StorageEngineInfo> storageEngineInfos,
      List<MetaStorageInfo> metaStorageInfo) {
    this(iginxInfos, storageEngineInfos, null, metaStorageInfo);
  }

  public List<IginxInfo> getIginxInfos() {
    return iginxInfos;
  }

  public List<StorageEngineInfo> getStorageEngineInfos() {
    return storageEngineInfos;
  }

  public LocalMetaStorageInfo getLocalMetaStorageInfo() {
    return localMetaStorageInfo;
  }

  public List<MetaStorageInfo> getMetaStorageInfos() {
    return metaStorageInfos;
  }

  public boolean isUseLocalMetaStorage() {
    return localMetaStorageInfo != null;
  }
}
