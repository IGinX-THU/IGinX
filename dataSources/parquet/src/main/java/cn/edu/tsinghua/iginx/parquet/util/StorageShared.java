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

import cn.edu.tsinghua.iginx.parquet.util.buffer.BufferPool;
import cn.edu.tsinghua.iginx.parquet.util.buffer.BufferPools;
import cn.edu.tsinghua.iginx.parquet.util.cache.CachePool;
import java.util.concurrent.Semaphore;

public class StorageShared {
  private final StorageProperties storageProperties;

  private final Semaphore flusherPermits;

  private final CachePool cachePool;

  private final BufferPool bufferPool;

  public StorageShared(
      StorageProperties storageProperties,
      Semaphore flusherPermits,
      CachePool cachePool,
      BufferPool bufferPool) {
    this.storageProperties = storageProperties;
    this.flusherPermits = flusherPermits;
    this.cachePool = cachePool;
    this.bufferPool = bufferPool;
  }

  public static StorageShared of(StorageProperties storageProperties) {
    Semaphore flusherPermits = new Semaphore(storageProperties.getCompactPermits(), true);
    CachePool cachePool = new CachePool(storageProperties);
    BufferPool bufferPool = BufferPools.from(storageProperties);
    return new StorageShared(storageProperties, flusherPermits, cachePool, bufferPool);
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

  public BufferPool getBufferPool() {
    return bufferPool;
  }
}
