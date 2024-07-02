package cn.edu.tsinghua.iginx.parquet.util;

import java.util.concurrent.Semaphore;

public class Shared {
  private final StorageProperties storageProperties;

  private final Semaphore flusherPermits;

  private final CachePool cachePool;

  public Shared(
      StorageProperties storageProperties, Semaphore flusherPermits, CachePool cachePool) {
    this.storageProperties = storageProperties;
    this.flusherPermits = flusherPermits;
    this.cachePool = cachePool;
  }

  public static Shared of(StorageProperties storageProperties) {
    Semaphore flusherPermits = new Semaphore(storageProperties.getCompactPermits(), true);
    CachePool cachePool = new CachePool(storageProperties);
    return new Shared(storageProperties, flusherPermits, cachePool);
  }

  public StorageProperties getStorageProperties() {
    return storageProperties;
  }

  public Semaphore getFlusherPermits() {
    return flusherPermits;
  }

  public CachePool getCachePool() {
    return cachePool;
  }
}
