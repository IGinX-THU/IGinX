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
