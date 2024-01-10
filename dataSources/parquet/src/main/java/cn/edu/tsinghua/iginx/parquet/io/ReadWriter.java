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
import cn.edu.tsinghua.iginx.parquet.common.scanner.Scanner;
import com.google.common.collect.Range;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

public interface ReadWriter<K extends Comparable<K>, F, V, T> {

  void flush(
      Path path, Scanner<K, Scanner<F, V>> scanner, Map<F, T> schema, Map<String, String> extra)
      throws IOException;

  Map.Entry<Map<F, T>, Map<String, String>> readMeta(Path path) throws IOException;

  Scanner<K, Scanner<F, V>> scanData(Path path, Set<F> fields, Range<K> range, Filter predicate)
      throws IOException;

  ObjectFormat<K> getKeyFormat();

  ObjectFormat<F> getFieldFormat();
}
