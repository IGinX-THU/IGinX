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

package cn.edu.tsinghua.iginx.parquet.io.parquet;

import cn.edu.tsinghua.iginx.parquet.entity.DataChunk;
import cn.edu.tsinghua.iginx.parquet.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.parquet.local.ParquetReadOptions;

class ParquetReader implements FileReader {

  public ParquetReader(
      Path path, ParquetMeta meta, ParquetIndex index, ParquetReadOptions options) {}

  @Nonnull
  @Override
  public Map<String, DataChunk> load(@Nonnull Set<String> fields, long limit) throws IOException {
    return null;
  }

  @Override
  public void seek(long start) throws IOException {}

  @Override
  public void close() throws IOException {}
}
