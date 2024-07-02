package cn.edu.tsinghua.iginx.metadata.hook;

import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;

public interface StorageUnitChangeHook {

  void onChange(String id, StorageUnitMeta storageUnit);
}
