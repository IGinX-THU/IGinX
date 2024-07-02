/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
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
