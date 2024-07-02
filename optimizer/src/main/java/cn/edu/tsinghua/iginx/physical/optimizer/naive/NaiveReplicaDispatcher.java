package cn.edu.tsinghua.iginx.physical.optimizer.naive;

import cn.edu.tsinghua.iginx.engine.physical.optimizer.ReplicaDispatcher;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;

public class NaiveReplicaDispatcher implements ReplicaDispatcher {

  private static final NaiveReplicaDispatcher INSTANCE = new NaiveReplicaDispatcher();

  private NaiveReplicaDispatcher() {}

  @Override
  public String chooseReplica(StoragePhysicalTask task) {
    if (task == null) {
      return null;
    }
    return task.getTargetFragment().getMasterStorageUnitId();
  }

  public static NaiveReplicaDispatcher getInstance() {
    return INSTANCE;
  }
}
