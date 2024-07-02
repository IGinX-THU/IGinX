package cn.edu.tsinghua.iginx.engine.physical.storage.domain;

import java.util.*;

public class ColumnKey {
  private final String path;
  private final SortedMap<String, String> tags;

  public ColumnKey(String path, Map<String, String> tagList) {
    this.path = Objects.requireNonNull(path);
    this.tags = Collections.unmodifiableSortedMap(new TreeMap<>(tagList));
  }

  public String getPath() {
    return path;
  }

  public SortedMap<String, String> getTags() {
    return tags;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ColumnKey columnKey = (ColumnKey) o;
    return Objects.equals(path, columnKey.path) && Objects.equals(tags, columnKey.tags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, tags);
  }

  @Override
  public String toString() {
    return path + tags;
  }
}
