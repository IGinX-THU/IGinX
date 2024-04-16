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

package cn.edu.tsinghua.iginx.parquet.io.common;

import cn.edu.tsinghua.iginx.parquet.db.util.iterator.EmptyScanner;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import javax.annotation.Nonnull;

public class DataChunks {
  private DataChunks() {}

  private static class EmptyDataChunk implements DataChunk {

    @Override
    public long bytes() {
      return 0;
    }


    @Override
    public Scanner<Long, Object> scan(long position) {
      return EmptyScanner.getInstance();
    }

    @Override
    public void close() throws Exception {}
  }

  private static final DataChunk EMPTY = new EmptyDataChunk();

  public static DataChunk empty() {
    return EMPTY;
  }
}
