/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util;

import java.util.Objects;
import org.apache.arrow.vector.VectorSchemaRoot;

public class ComputeResult implements AutoCloseable {

  private CloseableDictionaryProvider dictionary;
  private VectorSchemaRoot data;

  public ComputeResult(CloseableDictionaryProvider dictionary, VectorSchemaRoot data) {
    this.data = Objects.requireNonNull(data);
    this.dictionary = Objects.requireNonNull(dictionary);
  }

  public CloseableDictionaryProvider extractDictionaryProvider() {
    CloseableDictionaryProvider result = dictionary;
    dictionary = null;
    return result;
  }

  public VectorSchemaRoot extractData() {
    VectorSchemaRoot result = data;
    data = null;
    return result;
  }

  @Override
  public void close() {
    if (dictionary != null) {
      dictionary.close();
    }
    if (data != null) {
      data.close();
    }
  }
}
