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

import java.util.Collections;
import java.util.Set;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

public class DictionaryProviders {
  private DictionaryProviders() {}

  private static class EmptyDictionaryProvider implements CloseableDictionaryProvider {
    @Override
    public Dictionary lookup(long id) {
      return null;
    }

    @Override
    public Set<Long> getDictionaryIds() {
      return Collections.emptySet();
    }

    @Override
    public void close() {}

    @Override
    public CloseableDictionaryProvider slice(BufferAllocator allocator) {
      return this;
    }
  }

  private static final CloseableDictionaryProvider EMPTY_DICTIONARY_PROVIDER =
      new EmptyDictionaryProvider();

  public static DictionaryProvider empty() {
    return EMPTY_DICTIONARY_PROVIDER;
  }

  public static CloseableDictionaryProvider emptyClosable() {
    return EMPTY_DICTIONARY_PROVIDER;
  }
}
