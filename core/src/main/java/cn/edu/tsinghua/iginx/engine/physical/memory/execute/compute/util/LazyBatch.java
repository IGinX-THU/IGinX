/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util;

import static org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;

import java.util.Objects;
import javax.annotation.WillCloseWhenClosed;
import lombok.Getter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

@Getter
public class LazyBatch implements AutoCloseable {

  private final MapDictionaryProvider dictionaryProvider;

  private final VectorSchemaRoot data;

  public static LazyBatch slice(
      BufferAllocator allocator, DictionaryProvider dictionaryProvider, VectorSchemaRoot data) {
    return new LazyBatch(
        ArrowDictionaries.slice(allocator, dictionaryProvider, data.getSchema()),
        VectorSchemaRoots.slice(allocator, data));
  }

  public LazyBatch(
      @WillCloseWhenClosed MapDictionaryProvider dictionaryProvider,
      @WillCloseWhenClosed VectorSchemaRoot data) {
    this.dictionaryProvider = Objects.requireNonNull(dictionaryProvider);
    this.data = Objects.requireNonNull(data);
  }

  @Override
  public void close() {
    data.close();
    dictionaryProvider.close();
  }
}
