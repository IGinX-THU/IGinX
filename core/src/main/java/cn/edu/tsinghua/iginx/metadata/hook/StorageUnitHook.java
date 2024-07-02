package cn.edu.tsinghua.iginx.metadata.hook;

import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;

public interface StorageUnitHook {

  void onChange(StorageUnitMeta before, StorageUnitMeta after);
}
