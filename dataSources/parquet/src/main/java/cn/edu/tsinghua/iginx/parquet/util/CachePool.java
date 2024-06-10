/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.tsinghua.iginx.parquet.util;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import org.ehcache.sizeof.SizeOf;

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
    default int getWeight(){
      return Math.toIntExact(SizeOf.newInstance().deepSizeOf(this));
    }
  }
}
