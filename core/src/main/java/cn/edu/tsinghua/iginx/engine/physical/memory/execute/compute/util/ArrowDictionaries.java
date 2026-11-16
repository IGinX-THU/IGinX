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

import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.tuple.Pair;

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

  public static LazyBatch select(
      BufferAllocator allocator,
      DictionaryProvider dictionaryProvider,
      VectorSchemaRoot data,
      @Nullable BaseIntVector selection) {
    return select(allocator, dictionaryProvider, data, selection, null);
  }

  public static LazyBatch select(
      BufferAllocator allocator,
      DictionaryProvider dictionaryProvider,
      VectorSchemaRoot data,
      @Nullable BaseIntVector selection,
      @Nullable boolean[] nullColumns) {
    if (selection == null) {
      return LazyBatch.slice(allocator, dictionaryProvider, data);
    }
    long nextId =
        dictionaryProvider.getDictionaryIds().stream().max(Long::compareTo).orElse(0L) + 1;

    MapDictionaryProvider mapDictionaryProvider =
        slice(allocator, dictionaryProvider, data.getSchema());

    Map<Pair<Long, Long>, Integer> selectedCache = new HashMap<>();

    List<FieldVector> resultVectors = new ArrayList<>();
    List<FieldVector> sourceVectors = data.getFieldVectors();
    for (int i = 0; i < sourceVectors.size(); i++) {
      FieldVector vector = sourceVectors.get(i);
      Field field = vector.getField();
      if (nullColumns != null && nullColumns[i]) {
        resultVectors.add(ValueVectors.nullOf(vector.getName(), selection.getValueCount()));
        continue;
      }

      DictionaryEncoding dictionaryEncoding = vector.getField().getDictionary();

      if (dictionaryEncoding == null) {
        long id = nextId++;
        Dictionary newDictionary =
            new Dictionary(
                ValueVectors.slice(allocator, vector),
                new DictionaryEncoding(
                    id, false, (ArrowType.Int) (selection.getField().getType())));
        mapDictionaryProvider.put(newDictionary);
        resultVectors.add(sliceWithDictionary(allocator, selection, newDictionary, field));
        continue;
      }

      BaseIntVector indexVector = (BaseIntVector) vector;
      Pair<Long, Long> cacheKey =
          Pair.of(indexVector.getValidityBufferAddress(), indexVector.getDataBufferAddress());

      if (selectedCache.containsKey(cacheKey)) {
        Dictionary dictionary = mapDictionaryProvider.lookup(dictionaryEncoding.getId());
        BaseIntVector cachedSelectedIndexVector =
            (BaseIntVector) resultVectors.get(selectedCache.get(cacheKey));
        resultVectors.add(
            sliceWithDictionary(allocator, cachedSelectedIndexVector, dictionary, field));
        continue;
      }

      selectedCache.put(cacheKey, resultVectors.size());
      resultVectors.add(ValueVectors.select(allocator, indexVector, selection));
    }

    return new LazyBatch(
        mapDictionaryProvider, VectorSchemaRoots.create(resultVectors, selection.getValueCount()));
  }

  public static LazyBatch join(BufferAllocator allocator, LazyBatch... batches) {
    Preconditions.checkArgument(
        Arrays.stream(batches)
                .map(LazyBatch::getData)
                .mapToInt(VectorSchemaRoot::getRowCount)
                .distinct()
                .count()
            == 1);

    MapDictionaryProvider resultDictionaryProvider = new MapDictionaryProvider();
    List<FieldVector> resultVectors = new ArrayList<>();
    for (LazyBatch batch : batches) {
      for (FieldVector vector : batch.getData().getFieldVectors()) {
        DictionaryEncoding dictionaryEncoding = vector.getField().getDictionary();

        if (dictionaryEncoding == null) {
          resultVectors.add(ValueVectors.slice(allocator, vector));
          continue;
        }

        BaseIntVector indexVector = (BaseIntVector) vector;
        long id = dictionaryEncoding.getId();
        Dictionary dictionary = batch.getDictionaryProvider().lookup(id);

        long newId = resultVectors.size();
        Dictionary newDictionary =
            new Dictionary(
                ValueVectors.slice(allocator, dictionary.getVector()),
                new DictionaryEncoding(
                    newId,
                    dictionary.getEncoding().isOrdered(),
                    dictionary.getEncoding().getIndexType()));
        resultDictionaryProvider.put(newDictionary);
        resultVectors.add(
            sliceWithDictionary(allocator, indexVector, newDictionary, indexVector.getField()));
      }
    }
    return new LazyBatch(
        resultDictionaryProvider,
        VectorSchemaRoots.create(resultVectors, batches[0].getData().getRowCount()));
  }

  private static FieldVector sliceWithDictionary(
      BufferAllocator allocator, BaseIntVector indices, Dictionary dictionary, Field sourceField) {
    return ValueVectors.slice(
        allocator,
        indices,
        Schemas.fieldWith(
            indices.getField(),
            sourceField.getName(),
            dictionary.getEncoding(),
            sourceField.getMetadata()));
  }

  public static Field flatten(DictionaryProvider dictionaryProvider, Field field) {
    if (field.getDictionary() == null) {
      return field;
    }
    Dictionary dictionary = dictionaryProvider.lookup(field.getDictionary().getId());
    Field dictionaryField = dictionary.getVector().getField();
    return Schemas.fieldWithNullable(
        Schemas.fieldWithName(dictionaryField, field.getName()),
        field.isNullable() || dictionaryField.isNullable());
  }

  public static Schema flatten(DictionaryProvider dictionaryProvider, Schema field) {
    return new Schema(
        field.getFields().stream()
            .map(f -> flatten(dictionaryProvider, f))
            .collect(Collectors.toList()));
  }

  public static FieldVector flatten(
      BufferAllocator allocator,
      Dictionary dictionary,
      FieldVector vector,
      @Nullable BaseIntVector selection) {
    FieldVector dictionaryVector = dictionary.getVector();
    BaseIntVector indices = (BaseIntVector) vector;
    int destCount = selection == null ? vector.getValueCount() : selection.getValueCount();

    try (FieldVector dest = dictionaryVector.getField().createVector(allocator)) {
      FixedWidthVector fixedWidthVector =
          dest instanceof FixedWidthVector ? (FixedWidthVector) dest : null;
      if (fixedWidthVector != null) {
        fixedWidthVector.allocateNew(destCount);
      } else {
        dest.setInitialCapacity(destCount);
      }
      for (int destIndex = 0; destIndex < destCount; destIndex++) {
        int sourceIndex = selection == null ? destIndex : (int) selection.getValueAsLong(destIndex);
        if (indices.isNull(sourceIndex)) {
          continue;
        }
        int dictionaryIndex = (int) indices.getValueAsLong(sourceIndex);
        if (fixedWidthVector != null) {
          fixedWidthVector.copyFrom(dictionaryIndex, destIndex, dictionaryVector);
        } else {
          dest.copyFromSafe(dictionaryIndex, destIndex, dictionaryVector);
        }
      }
      dest.setValueCount(destCount);
      return ValueVectors.transfer(allocator, dest, indices.getName());
    }
  }

  public static FieldVector flatten(
      BufferAllocator allocator,
      DictionaryProvider dictionaryProvider,
      FieldVector vector,
      @Nullable BaseIntVector selection) {
    DictionaryEncoding dictionaryEncoding = vector.getField().getDictionary();
    if (dictionaryEncoding == null) {
      if (selection != null) {
        return ValueVectors.select(allocator, vector, selection);
      } else {
        return ValueVectors.slice(allocator, vector);
      }
    }

    Dictionary dictionary = dictionaryProvider.lookup(dictionaryEncoding.getId());
    return flatten(allocator, dictionary, vector, selection);
  }

  public static VectorSchemaRoot flatten(
      BufferAllocator allocator,
      DictionaryProvider dictionaryProvider,
      VectorSchemaRoot batch,
      @Nullable BaseIntVector selection) {
    List<FieldVector> resultFieldVectors = new ArrayList<>();
    for (FieldVector fieldVector : batch.getFieldVectors()) {
      resultFieldVectors.add(flatten(allocator, dictionaryProvider, fieldVector, selection));
    }
    return VectorSchemaRoots.create(
        resultFieldVectors, selection == null ? batch.getRowCount() : selection.getValueCount());
  }
}
