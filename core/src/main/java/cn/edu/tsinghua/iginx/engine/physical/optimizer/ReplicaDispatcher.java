package cn.edu.tsinghua.iginx.engine.physical.optimizer;

import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;

public interface ReplicaDispatcher {

  String chooseReplica(StoragePhysicalTask task);
}
