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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;

public class ArrowDictionaries {

  public static MapDictionaryProvider slice(
      BufferAllocator allocator, DictionaryProvider dictionaryProvider) {
    return slice(allocator, dictionaryProvider, dictionaryProvider.getDictionaryIds());
  }

  public static MapDictionaryProvider slice(
      BufferAllocator allocator, DictionaryProvider dictionaryProvider, Schema schema) {
    Set<Long> ids =
        schema.getFields().stream()
            .map(Field::getDictionary)
            .filter(Objects::nonNull)
            .map(DictionaryEncoding::getId)
            .collect(Collectors.toSet());
    return slice(allocator, dictionaryProvider, ids);
  }

  public static MapDictionaryProvider slice(
      BufferAllocator allocator,
      DictionaryProvider dictionaryProvider,
      Iterable<Long> remainingIds) {
    List<Dictionary> dictionaries = new ArrayList<>();
    try {
      for (Long id : remainingIds) {
        Dictionary dictionary = dictionaryProvider.lookup(id);
        if (dictionary == null) {
          throw new IllegalArgumentException("Dictionary with id " + id + " not found");
        }
        dictionaries.add(
            new Dictionary(
                ValueVectors.slice(allocator, dictionary.getVector()), dictionary.getEncoding()));
      }
    } catch (Exception e) {
      for (Dictionary dictionary : dictionaries) {
        dictionary.getVector().close();
      }
      throw e;
    }
    return new MapDictionaryProvider(dictionaries.toArray(new Dictionary[0]));
  }

  public static DictionaryProvider emptyProvider() {
    return new MapDictionaryProvider();
  }

  public static Pair<MapDictionaryProvider, VectorSchemaRoot> select(
      BufferAllocator allocator,
      DictionaryProvider dictionaryProvider,
      VectorSchemaRoot data,
      BaseIntVector selection) {
    Preconditions.checkNotNull(selection);

    long nextId = dictionaryProvider.getDictionaryIds().stream().max(Long::compareTo).orElse(0L) + 1;

    MapDictionaryProvider mapDictionaryProvider = slice(allocator, dictionaryProvider, data.getSchema());
    List<FieldVector> resultVectors = new ArrayList<>();
    for (FieldVector vector : data.getFieldVectors()) {
      if (vector.getField().getDictionary() == null) {
        long id = nextId++;
        Dictionary dictionary = new Dictionary(
            ValueVectors.slice(allocator, vector),
            new DictionaryEncoding(
                id,
                false,
                (ArrowType.Int) (selection.getField().getType())
            )
        );
        mapDictionaryProvider.put(dictionary);
        resultVectors.add(ValueVectors.slice(allocator, selection, dictionary));
      } else {
        resultVectors.add(ValueVectors.select(allocator, vector, selection));
      }
    }
    return Pair.of(mapDictionaryProvider, VectorSchemaRoots.create(resultVectors, selection.getValueCount()));
  }
}
