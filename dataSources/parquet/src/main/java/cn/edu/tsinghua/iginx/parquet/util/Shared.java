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
