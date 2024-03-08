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

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import java.io.IOException;
import javax.annotation.Nonnull;

public interface FileIndex {

  /**
   * estimate size of data index
   *
   * @return size of data index in bytes
   */
  long size();

  /**
   * detect row ranges
   *
   * @param filter filter
   * @return row ranges. key is start row offset, value is row number.
   * @throws IOException if an I/O error occurs
   */
  @Nonnull
  Scanner<Long, Long> detect(@Nonnull Filter filter) throws IOException;
}
