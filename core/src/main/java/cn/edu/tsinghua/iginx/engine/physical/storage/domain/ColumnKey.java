package cn.edu.tsinghua.iginx.engine.physical.storage.domain;

import java.text.ParseException;
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

  private static final char KV_SEPARATOR = '=';
  private static final char TAG_SEPARATOR = ',';
  private static final char ESCAPE_PREFIX = '\\';

  public static String escape(String in) {
    StringBuilder sb = new StringBuilder();
    escape(in, sb);
    return sb.toString();
  }

  private static void escape(String in, StringBuilder sb) {
    for (int i = 0; i < in.length(); i++) {
      char c = in.charAt(i);
      switch (c) {
        case KV_SEPARATOR:
        case TAG_SEPARATOR:
        case ESCAPE_PREFIX:
          sb.append(ESCAPE_PREFIX);
          sb.append(c);
          break;
        default:
          sb.append(c);
      }
    }
  }

  private static String unescape(CharSequence in, int start, int end) {
    StringBuilder sb = new StringBuilder();
    unescape(in, start, end, sb);
    return sb.toString();
  }

  private static void unescape(CharSequence in, int start, int end, StringBuilder sb) {
    boolean escaped = false;
    for (int i = start; i < end; i++) {
      char c = in.charAt(i);
      if (escaped) {
        sb.append(c);
        escaped = false;
      } else if (c == ESCAPE_PREFIX) {
        escaped = true;
      } else {
        sb.append(c);
      }
    }
    if (escaped) {
      throw new IllegalArgumentException("Missing escaped character");
    }
  }

  @Override
  public String toString() {
    return path + tags;
  }

  public String toIdentifier() {
    StringBuilder sb = new StringBuilder();
    escape(path, sb);
    for (Map.Entry<String, String> entry : tags.entrySet()) {
      sb.append(TAG_SEPARATOR);
      escape(entry.getKey(), sb);
      sb.append(KV_SEPARATOR);
      escape(entry.getValue(), sb);
    }
    return sb.toString();
  }

  public static ColumnKey parseIdentifier(String id) throws ParseException {
    List<Map.Entry<Integer, Integer>> tagRanges = getTagRanges(id, 0, id.length());

    String path = unescape(id, tagRanges.get(0).getKey(), tagRanges.get(0).getValue());
    Map<String, String> tags = new HashMap<>();
    for (int i = 1; i < tagRanges.size(); i++) {
      Map.Entry<Integer, Integer> tagRange = tagRanges.get(i);
      Map.Entry<String, String> tag = parseTag(id, tagRange.getKey(), tagRange.getValue());
      tags.put(tag.getKey(), tag.getValue());
    }

    return new ColumnKey(path, tags);
  }

  private static List<Map.Entry<Integer, Integer>> getTagRanges(CharSequence in, int start, int end)
      throws ParseException {
    List<Map.Entry<Integer, Integer>> tagRanges = new ArrayList<>();
    int tagStart = start;
    boolean escaped = false;
    for (int i = start; i < end; i++) {
      if (escaped) {
        escaped = false;
        continue;
      }
      switch (in.charAt(i)) {
        case ESCAPE_PREFIX:
          escaped = true;
          break;
        case TAG_SEPARATOR:
          tagRanges.add(new AbstractMap.SimpleImmutableEntry<>(tagStart, i));
          tagStart = i + 1;
          break;
      }
    }
    if (escaped) {
      throw new ParseException("Missing escaped character", end);
    }
    tagRanges.add(new AbstractMap.SimpleImmutableEntry<>(tagStart, end));
    return tagRanges;
  }

  private static Map.Entry<String, String> parseTag(String in, int start, int end)
      throws ParseException {
    int kvSeparator = -1;

    boolean escaped = false;
    for (int i = start; i < end; i++) {
      if (escaped) {
        escaped = false;
        continue;
      }
      switch (in.charAt(i)) {
        case ESCAPE_PREFIX:
          escaped = true;
          break;
        case KV_SEPARATOR:
          if (kvSeparator != -1) {
            throw new ParseException("Multiple key-value separators", i);
          }
          kvSeparator = i;
          break;
      }
    }

    if (escaped) {
      throw new ParseException("Missing escaped character", end);
    }

    if (kvSeparator == -1) {
      throw new ParseException("Missing key-value separator", end);
    }

    String key = unescape(in, start, kvSeparator);
    String value = unescape(in, kvSeparator + 1, end);

    return new AbstractMap.SimpleImmutableEntry<>(key, value);
  }
}
