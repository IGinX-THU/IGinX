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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.metadata.field.tagkv;

import cn.edu.tsinghua.iginx.engine.shared.operator.tag.*;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectOpenCustomHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.util.*;
import org.checkerframework.checker.nullness.qual.Nullable;

public class InvertedTagsSet {
  private final Object2ObjectOpenHashMap<
          String, Object2ObjectOpenHashMap<String, ObjectOpenCustomHashSet<Map<String, String>>>>
      invertedIndex = new Object2ObjectOpenHashMap<>();
  private final ObjectOpenHashSet<Map<String, String>> allTags = new ObjectOpenHashSet<>();

  public void add(Map<String, String> tags) {
    if (allTags.contains(tags)) {
      return;
    }
    allTags.add(tags);
    for (Map.Entry<String, String> entry : tags.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      invertedIndex
          .computeIfAbsent(key, k -> new Object2ObjectOpenHashMap<>())
          .computeIfAbsent(
              value, v -> new ObjectOpenCustomHashSet<>(IdentityStrategy.getInstance()))
          .add(tags);
    }
  }

  public boolean contain(Map<String, String> tags) {
    return allTags.contains(tags);
  }

  public boolean isEmpty() {
    return allTags.isEmpty();
  }

  public void remove(Map<String, String> tags) {
    Map<String, String> existingTags = allTags.get(tags);
    if (existingTags == null) {
      return;
    }
    allTags.remove(existingTags);
    for (Map.Entry<String, String> entry : tags.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      Set<Map<String, String>> tagList = invertedIndex.get(key).get(value);
      tagList.remove(existingTags);
      if (tagList.isEmpty()) {
        invertedIndex.get(key).remove(value);
        if (invertedIndex.get(key).isEmpty()) {
          invertedIndex.remove(key);
        }
      }
    }
  }

  public Set<Map<String, String>> find(@Nullable TagFilter filter) {
    if (filter == null) {
      return Collections.unmodifiableSet(allTags);
    }
    switch (filter.getType()) {
      case Base:
        return find((BaseTagFilter) filter);
      case And:
        return find((AndTagFilter) filter);
      case Or:
        return find((OrTagFilter) filter);
      case BasePrecise:
        return find((BasePreciseTagFilter) filter);
      case Precise:
        return find((PreciseTagFilter) filter);
      case WithoutTag:
        return find((WithoutTagFilter) filter);
      default:
        throw new IllegalArgumentException("Unknown filter type: " + filter.getType());
    }
  }

  private Set<Map<String, String>> find(BaseTagFilter filter) {
    String key = filter.getTagKey();
    String value = filter.getTagValue();
    Object2ObjectOpenHashMap<String, ObjectOpenCustomHashSet<Map<String, String>>> forKey =
        invertedIndex.get((key));
    if (forKey != null) {
      if (value.equals("*")) {
        Set<Map<String, String>> result =
            new ObjectOpenCustomHashSet<>(IdentityStrategy.getInstance());
        for (ObjectOpenCustomHashSet<Map<String, String>> tagSet : forKey.values()) {
          result.addAll(tagSet);
        }
        return result;
      } else {
        ObjectOpenCustomHashSet<Map<String, String>> forKeyValue = forKey.get(value);
        if (forKeyValue != null) {
          return Collections.unmodifiableSet(forKeyValue);
        }
      }
    }
    return Collections.emptySet();
  }

  private Set<Map<String, String>> find(AndTagFilter filter) {
    List<Set<Map<String, String>>> partialResults = new ArrayList<>();
    for (TagFilter baseFilter : filter.getChildren()) {
      Set<Map<String, String>> partialResult = find(baseFilter);
      partialResults.add(partialResult);
    }
    if (partialResults.isEmpty()) {
      return Collections.emptySet();
    }
    Set<Map<String, String>> minPartialResult =
        partialResults.stream().min(Comparator.comparingInt(Set::size)).get();
    Set<Map<String, String>> result = new ObjectOpenCustomHashSet<>(IdentityStrategy.getInstance());
    for (Map<String, String> tags : minPartialResult) {
      boolean match = true;
      for (Set<Map<String, String>> partialResult : partialResults) {
        if (partialResult == minPartialResult) {
          continue;
        }
        if (!partialResult.contains(tags)) {
          match = false;
          break;
        }
      }
      if (match) {
        result.add(tags);
      }
    }
    return result;
  }

  private Set<Map<String, String>> find(OrTagFilter filter) {
    Set<Map<String, String>> result = new ObjectOpenCustomHashSet<>(IdentityStrategy.getInstance());
    for (TagFilter baseFilter : filter.getChildren()) {
      Set<Map<String, String>> partialResult = find(baseFilter);
      result.addAll(partialResult);
    }
    return result;
  }

  private Set<Map<String, String>> find(BasePreciseTagFilter filter) {
    if (allTags.contains(filter.getTags())) {
      return Collections.singleton(filter.getTags());
    } else {
      return Collections.emptySet();
    }
  }

  private Set<Map<String, String>> find(PreciseTagFilter filter) {
    Set<Map<String, String>> result = new ObjectOpenCustomHashSet<>(IdentityStrategy.getInstance());
    for (TagFilter baseFilter : filter.getChildren()) {
      Set<Map<String, String>> partialResult = find(baseFilter);
      result.addAll(partialResult);
    }
    return result;
  }

  private Set<Map<String, String>> find(WithoutTagFilter filter) {
    if (allTags.contains(Collections.emptyMap())) {
      return Collections.singleton(Collections.emptyMap());
    } else {
      return Collections.emptySet();
    }
  }

  public int size() {
    return allTags.size();
  }

  private static final class IdentityStrategy implements Hash.Strategy<Object> {

    private static final IdentityStrategy INSTANCE = new IdentityStrategy();

    public static IdentityStrategy getInstance() {
      return INSTANCE;
    }

    private IdentityStrategy() {}

    @Override
    public int hashCode(final Object o) {
      return System.identityHashCode(o);
    }

    @Override
    public boolean equals(final Object a, final Object b) {
      return a == b;
    }
  }
}
