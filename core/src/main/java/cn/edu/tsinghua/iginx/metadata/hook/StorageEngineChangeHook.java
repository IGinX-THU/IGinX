package cn.edu.tsinghua.iginx.metadata.hook;

import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;

public interface StorageEngineChangeHook {

  void onChange(StorageEngineMeta before, StorageEngineMeta after);
}
