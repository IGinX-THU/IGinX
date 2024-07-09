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

package cn.edu.tsinghua.iginx.parquet.db.lsm.table;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.parquet.db.lsm.api.TableMeta;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.LazyRowScanner;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.parquet.util.exception.StorageException;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.util.Set;
import javax.annotation.Nullable;

public interface Table {

  TableMeta getMeta() throws IOException;

  Scanner<Long, Scanner<String, Object>> scan(
      Set<String> fields, RangeSet<Long> range, @Nullable Filter superSetPredicate)
      throws IOException;

  default Scanner<Long, Scanner<String, Object>> scan(Set<String> fields, RangeSet<Long> ranges)
      throws IOException {
    return scan(fields, ranges, null);
  }

  default Scanner<Long, Scanner<String, Object>> lazyScan(
      Set<String> fields, RangeSet<Long> ranges) {
    return new LazyRowScanner<>(
        () -> {
          try {
            return scan(fields, ranges);
          } catch (IOException e) {
            throw new StorageException(e);
          }
        });
  }
}
