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

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.Scanner;
import cn.edu.tsinghua.iginx.parquet.db.lsm.iterator.IteratorScanner;
import cn.edu.tsinghua.iginx.parquet.io.FileIndex;
import cn.edu.tsinghua.iginx.parquet.io.FileMeta;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import javax.annotation.Nonnull;
import org.apache.parquet.local.ParquetReadOptions;

public class ParquetIndex implements FileIndex {
  final Path path;

  public ParquetIndex(Path path, FileMeta meta, ParquetReadOptions options) {
    this.path = path;
  }

  @Override
  public long size() {
    return 0;
  }

  @Nonnull
  @Override
  public Scanner<Long, Long> detect(@Nonnull Filter filter) throws IOException {
    return new IteratorScanner<>(
        Collections.singletonMap(0L, Long.MAX_VALUE).entrySet().iterator());
  }
}
