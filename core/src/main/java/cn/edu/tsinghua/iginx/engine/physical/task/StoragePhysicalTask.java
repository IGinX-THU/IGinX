package cn.edu.tsinghua.iginx.engine.physical.task;

import cn.edu.tsinghua.iginx.engine.physical.task.visitor.TaskVisitor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import java.util.List;

public class StoragePhysicalTask extends AbstractPhysicalTask {

  private final FragmentMeta targetFragment;
  private final boolean sync;
  private final boolean needBroadcasting;
  private String storageUnit;
  private long storage;
  private boolean dummyStorageUnit;

  public StoragePhysicalTask(List<Operator> operators, RequestContext context) {
    this(
        operators,
        ((FragmentSource) ((UnaryOperator) operators.get(0)).getSource()).getFragment(),
        true,
        false,
        context);
  }

  public StoragePhysicalTask(
      List<Operator> operators, boolean sync, boolean needBroadcasting, RequestContext context) {
    this(
        operators,
        ((FragmentSource) ((UnaryOperator) operators.get(0)).getSource()).getFragment(),
        sync,
        needBroadcasting,
        context);
  }

  public StoragePhysicalTask(
      List<Operator> operators,
      FragmentMeta targetFragment,
      boolean sync,
      boolean needBroadcasting,
      RequestContext context) {
    super(TaskType.Storage, operators, context);
    this.targetFragment = targetFragment;
    this.sync = sync;
    this.needBroadcasting = needBroadcasting;
  }

  public FragmentMeta getTargetFragment() {
    return targetFragment;
  }

  public String getStorageUnit() {
    return storageUnit;
  }

  public void setStorageUnit(String storageUnit) {
    this.storageUnit = storageUnit;
  }

  public boolean isDummyStorageUnit() {
    return dummyStorageUnit;
  }

  public void setDummyStorageUnit(boolean dummyStorageUnit) {
    this.dummyStorageUnit = dummyStorageUnit;
  }

  public long getStorage() {
    return storage;
  }

  public void setStorage(long storage) {
    this.storage = storage;
  }

  public boolean isSync() {
    return sync;
  }

  public boolean isNeedBroadcasting() {
    return needBroadcasting;
  }

  @Override
  public String toString() {
    return "StoragePhysicalTask{"
        + "targetFragment="
        + targetFragment
        + ", storageUnit='"
        + storageUnit
        + '\''
        + ", storage="
        + storage
        + '}';
  }

  @Override
  public void accept(TaskVisitor visitor) {
    visitor.enter();
    visitor.visit(this);
    visitor.leave();
  }
}
