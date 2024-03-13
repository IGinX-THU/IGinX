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

package cn.edu.tsinghua.iginx.parquet.util.buffer;

import cn.edu.tsinghua.iginx.parquet.util.StorageProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferPools {
  private static final Logger LOGGER = LoggerFactory.getLogger(BufferPools.class);

  private BufferPools() {}

  public static BufferPool from(StorageProperties storageProperties) {
    BufferPool bufferPool = new HeapPool();
    if (storageProperties.getPoolBufferRecycleEnable()) {
      LOGGER.info(
          "Recycle pool enabled, align: {}, limit: {}",
          storageProperties.getPoolBufferRecycleAlign(),
          storageProperties.getPoolBufferRecycleLimit());
      bufferPool =
          new RecyclePool(
              bufferPool,
              storageProperties.getPoolBufferRecycleAlign(),
              storageProperties.getPoolBufferRecycleLimit());
    }
    return bufferPool;
  }
}
