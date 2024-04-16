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

import java.util.concurrent.Semaphore;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

public class Shared {
  private final StorageProperties storageProperties;

  private final Semaphore flusherPermits;

  private final CachePool cachePool;

  private final BufferAllocator allocator;

  public Shared(
      StorageProperties storageProperties,
      Semaphore flusherPermits,
      CachePool cachePool,
      BufferAllocator allocator) {
    this.storageProperties = storageProperties;
    this.flusherPermits = flusherPermits;
    this.cachePool = cachePool;
    this.allocator = allocator;
  }

  public static Shared of(StorageProperties storageProperties) {
    Semaphore flusherPermits = new Semaphore(storageProperties.getCompactPermits(), true);
    CachePool cachePool = new CachePool(storageProperties);
    BufferAllocator allocator = new RootAllocator();
    return new Shared(storageProperties, flusherPermits, cachePool, allocator);
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

  public BufferAllocator getAllocator() {
    return allocator;
  }
}
