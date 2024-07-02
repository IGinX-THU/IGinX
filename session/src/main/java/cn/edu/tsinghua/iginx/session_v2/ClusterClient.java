package cn.edu.tsinghua.iginx.session_v2;

import cn.edu.tsinghua.iginx.session_v2.domain.ClusterInfo;
import cn.edu.tsinghua.iginx.session_v2.domain.Storage;
import cn.edu.tsinghua.iginx.session_v2.exception.IginXException;
import java.util.List;

public interface ClusterClient {

  ClusterInfo getClusterInfo() throws IginXException;

  void scaleOutStorage(final Storage storage) throws IginXException;

  void scaleOutStorages(final List<Storage> storages) throws IginXException;

  int getReplicaNum() throws IginXException;
}
