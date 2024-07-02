package cn.edu.tsinghua.iginx.metadata.hook;

import cn.edu.tsinghua.iginx.metadata.utils.ReshardStatus;

public interface ReshardStatusChangeHook {

  void onChange(ReshardStatus status);
}
