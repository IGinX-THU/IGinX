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

import cn.edu.tsinghua.iginx.parquet.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

public class EmptyReader implements FileReader {
  private EmptyReader() {}

  public static EmptyReader getInstance() {
    return EmptyReader.INSTANCE;
  }

  private static final EmptyReader INSTANCE = new EmptyReader();

  @Nonnull
  @Override
  public Map<String, DataChunk> load(@Nonnull Set<String> fields, long limit) throws IOException {
    Map<String, DataChunk> result = new HashMap<>();
    for (String field : fields) {
      result.put(field, DataChunks.empty());
    }
    return result;
  }

  @Override
  public void seek(long start) throws IOException {
    throw new IOException("EmptyReader is not seekable");
  }

  @Override
  public void close() throws IOException {}
}
