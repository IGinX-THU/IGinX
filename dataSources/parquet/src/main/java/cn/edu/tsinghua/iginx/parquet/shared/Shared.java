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

package cn.edu.tsinghua.iginx.parquet.shared;

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
    Semaphore flusherPermits = new Semaphore(storageProperties.getFlusherPermits(), true);
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
