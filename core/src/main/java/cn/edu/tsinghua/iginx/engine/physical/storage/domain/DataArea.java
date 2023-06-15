package cn.edu.tsinghua.iginx.engine.physical.storage.domain;

import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;

public class DataArea {

  private final String storageUnit;

  private final KeyInterval keyInterval;

  public DataArea(String storageUnit, KeyInterval keyInterval) {
    this.storageUnit = storageUnit;
    this.keyInterval = keyInterval;
  }

  public String getStorageUnit() {
    return storageUnit;
  }

  public KeyInterval getKeyInterval() {
    return keyInterval;
  }
}
