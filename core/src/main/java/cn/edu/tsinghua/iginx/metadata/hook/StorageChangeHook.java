package cn.edu.tsinghua.iginx.metadata.hook;

import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;

public interface StorageChangeHook {

  void onChange(long id, StorageEngineMeta storageEngine);
}
