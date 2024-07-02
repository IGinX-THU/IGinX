package cn.edu.tsinghua.iginx.parquet.util;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;

public class CachePool {

  private final Cache<String, Cacheable> cache;

  public CachePool(StorageProperties prop) {
    Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder();
    cacheBuilder.weigher((String name, Cacheable cacheable) -> cacheable.getWeight());
    cacheBuilder.maximumWeight(prop.getCacheCapacity());
    prop.getCacheTimeout().ifPresent(cacheBuilder::expireAfterAccess);
    cacheBuilder.scheduler(
        Scheduler.forScheduledExecutorService(Executors.newSingleThreadScheduledExecutor()));
    if (prop.getCacheSoftValues()) {
      cacheBuilder.softValues();
    }
    this.cache = cacheBuilder.build();
  }

  public ConcurrentMap<String, Cacheable> asMap() {
    return cache.asMap();
  }

  public interface Cacheable {
    int getWeight();
  }
}
