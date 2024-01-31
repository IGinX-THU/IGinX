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

import cn.edu.tsinghua.iginx.parquet.db.lsm.api.TableMeta;
import com.google.common.collect.Range;
import java.util.Map;

public class MemoryTableMeta<K extends Comparable<K>, F, T, V> implements TableMeta<K, F, T, V> {
  private final Map<F, T> schema;
  private final Map<F, Range<K>> ranges;

  public MemoryTableMeta(Map<F, T> schema, Map<F, Range<K>> ranges) {
    this.schema = schema;
    this.ranges = ranges;
  }

  public Map<F, T> getSchema() {
    return schema;
  }

  public Map<F, Range<K>> getRanges() {
    return ranges;
  }
}
