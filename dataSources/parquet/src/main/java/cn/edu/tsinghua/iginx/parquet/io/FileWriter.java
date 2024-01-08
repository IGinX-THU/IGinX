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

package cn.edu.tsinghua.iginx.parquet.io;

import cn.edu.tsinghua.iginx.parquet.io.common.DataChunk;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

public interface FileWriter extends Closeable {

  /**
   * start row offset to read
   *
   * @param start start row offset to read
   */
  void seek(long start) throws IOException;

  /**
   * dump data chunks into current row offset
   *
   * @param data data chunks to dump. key is field name, value is data chunk. row number of each
   *     data chunk must be same.
   */
  void dump(Map<String, DataChunk> data, long limit) throws IOException;
}
