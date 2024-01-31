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

package cn.edu.tsinghua.iginx.parquet.utils.cache;

import cn.edu.tsinghua.iginx.parquet.utils.StorageProperties;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class CachePool {

  private final Cache<String, Cacheable> cache;

  public CachePool(StorageProperties prop) {
    this.cache = CacheBuilder.newBuilder().build();
  }

  public Cacheable get(String name, Callable<? extends Cacheable> loader)
      throws ExecutionException {
    return cache.get(name, loader);
  }

  public void refresh(String name, Cacheable cacheable) {
    cache.put(name, cacheable);
  }

  public void invalidate(String name) {
    cache.invalidate(name);
  }
}
