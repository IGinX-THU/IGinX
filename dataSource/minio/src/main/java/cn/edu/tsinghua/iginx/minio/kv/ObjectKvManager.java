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
package cn.edu.tsinghua.iginx.minio.kv;

import cn.edu.tsinghua.iginx.engine.physical.storage.domain.ColumnKey;
import cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.minio.common.KeyRanges;
import cn.edu.tsinghua.iginx.minio.common.MinioCalls;
import cn.edu.tsinghua.iginx.minio.common.Patterns;
import cn.edu.tsinghua.iginx.minio.struct.DataBoundary;
import cn.edu.tsinghua.iginx.minio.struct.DataTarget;
import cn.edu.tsinghua.iginx.minio.struct.ObjectManager;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import io.minio.*;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import io.minio.messages.Item;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores columns as objects, like a key-value store.
 *
 * <p>Layout under {@code root} of {@code bucket}:
 *
 * <pre>
 * root/&lt;url-encoded column full name&gt;/&lt;segment name&gt;
 * </pre>
 *
 * Each insert writes one immutable segment per column; reading merges all segments of the column
 * (greater segment id wins on the same key). When a column has too many segments written by this
 * process, they are compacted into one. Deleting a key range rewrites the column.
 */
public class ObjectKvManager implements ObjectManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ObjectKvManager.class);

  private static final String SEPARATOR = "/";
  private static final int MAX_DELETE_BATCH = 1000;
  private static final int MAX_READ_RETRY = 3;

  private final MinioClient client;
  private final String bucket;
  private final String root;
  private final ExecutorService executor;
  private final int compactThreshold;

  private final ConcurrentHashMap<String, AtomicInteger> uncompacted = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Object> locks = new ConcurrentHashMap<>();

  /**
   * @param root key prefix of this manager, empty or ending with "/"
   * @param compactThreshold compact a column when this process has written so many segments of it
   */
  public ObjectKvManager(
      MinioClient client,
      String bucket,
      String root,
      ExecutorService executor,
      int compactThreshold) {
    this.client = Objects.requireNonNull(client);
    this.bucket = Objects.requireNonNull(bucket);
    this.root = normalizeRoot(root);
    this.executor = Objects.requireNonNull(executor);
    this.compactThreshold = compactThreshold;
  }

  public static String normalizeRoot(@Nullable String root) {
    if (root == null) {
      return "";
    }
    String trimmed = root;
    while (trimmed.startsWith(SEPARATOR)) {
      trimmed = trimmed.substring(1);
    }
    while (trimmed.endsWith(SEPARATOR)) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    return trimmed.isEmpty() ? "" : trimmed + SEPARATOR;
  }

  public String getBucket() {
    return bucket;
  }

  public String getRoot() {
    return root;
  }

  /** Objects of one column. */
  static final class ColumnObjects {
    final String fullName;
    final String path;
    final Map<String, String> tags;
    final List<SegmentName> segments = new ArrayList<>();
    DataType type;

    ColumnObjects(String fullName, ColumnKey key) {
      this.fullName = fullName;
      this.path = key.getPath();
      this.tags = key.getTags();
    }

    /** keep only segments of the newest type, sorted by id */
    void seal() {
      segments.sort(Comparator.naturalOrder());
      type = segments.get(segments.size() - 1).getType();
    }

    List<SegmentName> typedSegments() {
      List<SegmentName> typed = new ArrayList<>();
      for (SegmentName segment : segments) {
        if (segment.getType() == type) {
          typed.add(segment);
        }
      }
      return typed;
    }
  }

  private String columnPrefix(String fullName) {
    return root + ColumnNames.encode(fullName) + SEPARATOR;
  }

  private String objectKey(String fullName, SegmentName segment) {
    return columnPrefix(fullName) + segment.toFileName();
  }

  /** list columns whose path matches any pattern (null means all) and tags match tagFilter */
  NavigableMap<String, ColumnObjects> listColumns(
      @Nullable Collection<String> patterns, @Nullable TagFilter tagFilter) throws IOException {
    List<String> prefixes = new ArrayList<>();
    if (patterns == null || Patterns.isAll(patterns)) {
      prefixes.add(root);
    } else {
      TreeSet<String> sorted = new TreeSet<>();
      for (String pattern : patterns) {
        sorted.add(root + ColumnNames.encodedLiteralPrefix(pattern));
      }
      for (String prefix : sorted) {
        if (prefixes.isEmpty() || !prefix.startsWith(prefixes.get(prefixes.size() - 1))) {
          prefixes.add(prefix);
        }
      }
    }

    NavigableMap<String, ColumnObjects> columns = new TreeMap<>();
    for (String prefix : prefixes) {
      for (Result<Item> result :
          client.listObjects(
              ListObjectsArgs.builder().bucket(bucket).prefix(prefix).recursive(true).build())) {
        Item item;
        try {
          item = MinioCalls.call(result::get);
        } catch (NoSuchFileException e) {
          return new TreeMap<>();
        }
        String relative = item.objectName().substring(root.length());
        int split = relative.indexOf(SEPARATOR);
        if (split <= 0 || relative.indexOf(SEPARATOR, split + 1) >= 0) {
          continue;
        }
        SegmentName segment = SegmentName.parse(relative.substring(split + 1));
        if (segment == null) {
          continue;
        }
        String fullName = ColumnNames.decode(relative.substring(0, split));
        ColumnObjects column = columns.get(fullName);
        if (column == null) {
          ColumnKey key;
          try {
            key = ColumnNames.fromFullName(fullName);
          } catch (IllegalStateException e) {
            LOGGER.warn("skip object with invalid column name: {}", item.objectName());
            continue;
          }
          if (patterns != null && !Patterns.match(patterns, key.getPath())) {
            continue;
          }
          if (tagFilter != null && !TagKVUtils.match(key.getTags(), tagFilter)) {
            continue;
          }
          column = new ColumnObjects(fullName, key);
          columns.put(fullName, column);
        }
        column.segments.add(segment);
      }
    }
    columns.values().forEach(ColumnObjects::seal);
    return columns;
  }

  @Nullable
  private ColumnObjects listColumn(String fullName) throws IOException {
    ColumnObjects column = new ColumnObjects(fullName, ColumnNames.fromFullName(fullName));
    String prefix = columnPrefix(fullName);
    for (Result<Item> result :
        client.listObjects(
            ListObjectsArgs.builder().bucket(bucket).prefix(prefix).recursive(true).build())) {
      Item item = MinioCalls.call(result::get);
      SegmentName segment = SegmentName.parse(item.objectName().substring(prefix.length()));
      if (segment != null) {
        column.segments.add(segment);
      }
    }
    if (column.segments.isEmpty()) {
      return null;
    }
    column.seal();
    return column;
  }

  @Override
  public DataBoundary getBoundary(@Nullable String prefix) throws IOException {
    List<String> patterns = prefix == null ? null : Collections.singletonList(prefix + "*");
    String min = null;
    String max = null;
    for (ColumnObjects column : listColumns(patterns, null).values()) {
      if (prefix != null && !column.path.equals(prefix) && !column.path.startsWith(prefix + ".")) {
        continue;
      }
      if (min == null || column.path.compareTo(min) < 0) {
        min = column.path;
      }
      if (max == null || column.path.compareTo(max) > 0) {
        max = column.path;
      }
    }
    if (min == null) {
      return DataBoundary.empty();
    }
    return DataBoundary.of(min, StringUtils.nextString(max));
  }

  @Override
  public RowStream query(DataTarget target) throws IOException {
    List<String> patterns = target.getPatterns();
    if (patterns != null && patterns.isEmpty()) {
      return new ColumnsRowStream(
          new Header(Field.KEY, Collections.emptyList()), Collections.emptyList(), null);
    }

    List<ColumnObjects> columns =
        new ArrayList<>(listColumns(patterns, target.getTagFilter()).values());
    List<Field> fields = new ArrayList<>();
    for (ColumnObjects column : columns) {
      fields.add(new Field(column.path, column.type, column.tags));
    }
    Header header = new Header(Field.KEY, fields);

    RangeSet<Long> ranges = KeyRanges.of(target.getFilter());
    List<Segment> loaded = new ArrayList<>();
    if (ranges.isEmpty()) {
      for (ColumnObjects column : columns) {
        loaded.add(new Segment(column.type, new long[0], new Object[0]));
      }
    } else {
      List<Callable<Segment>> tasks = new ArrayList<>();
      for (ColumnObjects column : columns) {
        tasks.add(() -> loadColumn(column, ranges));
      }
      loaded = invokeAll(tasks);
    }
    return new ColumnsRowStream(header, loaded, target.getFilter());
  }

  private Segment loadColumn(ColumnObjects column, RangeSet<Long> ranges) throws IOException {
    ColumnObjects current = column;
    for (int retry = 0; ; retry++) {
      try {
        NavigableMap<Long, Object> merged = mergeSegments(current, ranges);
        return Segment.of(column.type, merged);
      } catch (NoSuchFileException e) {
        // segments are replaced by compaction or deletion concurrently
        if (retry >= MAX_READ_RETRY) {
          throw e;
        }
        current = listColumn(column.fullName);
        if (current == null || current.type != column.type) {
          return new Segment(column.type, new long[0], new Object[0]);
        }
      }
    }
  }

  private NavigableMap<Long, Object> mergeSegments(ColumnObjects column, RangeSet<Long> ranges)
      throws IOException {
    NavigableMap<Long, Object> merged = new TreeMap<>();
    for (SegmentName name : column.typedSegments()) {
      if (!KeyRanges.intersects(ranges, name.getMinKey(), name.getMaxKey())) {
        continue;
      }
      Segment segment = readSegment(objectKey(column.fullName, name));
      long[] keys = segment.getKeys();
      Object[] values = segment.getValues();
      boolean all = ranges.encloses(Range.closed(name.getMinKey(), name.getMaxKey()));
      for (int i = 0; i < keys.length; i++) {
        if (all || ranges.contains(keys[i])) {
          merged.put(keys[i], values[i]);
        }
      }
    }
    return merged;
  }

  private Segment readSegment(String objectKey) throws IOException {
    byte[] content =
        MinioCalls.call(
            () -> {
              try (InputStream in =
                  client.getObject(
                      GetObjectArgs.builder().bucket(bucket).object(objectKey).build())) {
                return readAll(in);
              }
            });
    return Segment.decode(content);
  }

  private static byte[] readAll(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[64 * 1024];
    int n;
    while ((n = in.read(buffer)) >= 0) {
      out.write(buffer, 0, n);
    }
    return out.toByteArray();
  }

  private void writeSegment(String fullName, String id, Segment segment) throws IOException {
    SegmentName name = new SegmentName(id, segment.getType(), segment.minKey(), segment.maxKey());
    byte[] content = segment.encode();
    String key = objectKey(fullName, name);
    MinioCalls.run(
        () ->
            client.putObject(
                PutObjectArgs.builder().bucket(bucket).object(key).stream(
                        new ByteArrayInputStream(content), content.length, -1)
                    .contentType("application/octet-stream")
                    .build()));
  }

  @Override
  public void insert(DataView data) throws IOException {
    int pathNum = data.getPathNum();
    List<NavigableMap<Long, Object>> columns = new ArrayList<>(pathNum);
    for (int i = 0; i < pathNum; i++) {
      columns.add(new TreeMap<>());
    }
    if (data.isRowData()) {
      for (int row = 0; row < data.getKeySize(); row++) {
        BitmapView bitmap = data.getBitmapView(row);
        long key = data.getKey(row);
        int index = 0;
        for (int col = 0; col < pathNum; col++) {
          if (bitmap.get(col)) {
            columns.get(col).put(key, data.getValue(row, index++));
          }
        }
      }
    } else {
      for (int col = 0; col < pathNum; col++) {
        BitmapView bitmap = data.getBitmapView(col);
        int index = 0;
        for (int row = 0; row < data.getKeySize(); row++) {
          if (bitmap.get(row)) {
            columns.get(col).put(data.getKey(row), data.getValue(col, index++));
          }
        }
      }
    }

    List<Callable<Void>> tasks = new ArrayList<>();
    for (int col = 0; col < pathNum; col++) {
      NavigableMap<Long, Object> values = columns.get(col);
      if (values.isEmpty()) {
        continue;
      }
      String fullName = ColumnNames.toFullName(data.getPath(col), data.getTags(col));
      Segment segment = Segment.of(data.getDataType(col), values);
      tasks.add(
          () -> {
            writeSegment(fullName, SegmentName.newId(), segment);
            maybeCompact(fullName);
            return null;
          });
    }
    invokeAll(tasks);
  }

  private void maybeCompact(String fullName) throws IOException {
    AtomicInteger counter = uncompacted.computeIfAbsent(fullName, k -> new AtomicInteger());
    if (counter.incrementAndGet() < compactThreshold) {
      return;
    }
    synchronized (lockOf(fullName)) {
      if (counter.get() < compactThreshold) {
        return;
      }
      counter.set(0);
      ColumnObjects column = listColumn(fullName);
      if (column == null || column.segments.size() <= 1) {
        return;
      }
      LOGGER.debug("compact {} segments of {} in {}", column.segments.size(), fullName, root);
      rewrite(column, null);
    }
  }

  /** replace all segments of column by one segment, excluding keys in deleted ranges */
  private void rewrite(ColumnObjects column, @Nullable RangeSet<Long> deleted) throws IOException {
    RangeSet<Long> kept =
        deleted == null ? ImmutableRangeSet.of(Range.all()) : deleted.complement();
    NavigableMap<Long, Object> merged = mergeSegments(column, kept);
    List<String> obsolete = new ArrayList<>();
    for (SegmentName name : column.segments) {
      obsolete.add(objectKey(column.fullName, name));
    }
    if (!merged.isEmpty()) {
      String newestId = column.segments.get(column.segments.size() - 1).getId();
      Segment segment = Segment.of(column.type, merged);
      writeSegment(column.fullName, newestId, segment);
      obsolete.remove(
          objectKey(
              column.fullName,
              new SegmentName(newestId, column.type, segment.minKey(), segment.maxKey())));
    }
    removeObjects(obsolete);
  }

  @Override
  public void delete(DataTarget target) throws IOException {
    boolean allColumns = Patterns.isAll(target.getPatterns()) && target.getTagFilter() == null;
    Filter filter = target.getFilter();
    if (allColumns && filter == null) {
      removeAll(root);
      uncompacted.clear();
      return;
    }
    Collection<ColumnObjects> columns =
        listColumns(target.getPatterns(), target.getTagFilter()).values();
    if (filter == null) {
      List<String> keys = new ArrayList<>();
      for (ColumnObjects column : columns) {
        for (SegmentName name : column.segments) {
          keys.add(objectKey(column.fullName, name));
        }
        uncompacted.remove(column.fullName);
      }
      removeObjects(keys);
      return;
    }
    RangeSet<Long> deleted = KeyRanges.of(filter);
    if (deleted.isEmpty()) {
      return;
    }
    List<Callable<Void>> tasks = new ArrayList<>();
    for (ColumnObjects column : columns) {
      tasks.add(
          () -> {
            synchronized (lockOf(column.fullName)) {
              ColumnObjects current = listColumn(column.fullName);
              if (current != null) {
                rewrite(current, deleted);
              }
            }
            return null;
          });
    }
    invokeAll(tasks);
  }

  private void removeAll(String prefix) throws IOException {
    List<String> keys = new ArrayList<>();
    for (Result<Item> result :
        client.listObjects(
            ListObjectsArgs.builder().bucket(bucket).prefix(prefix).recursive(true).build())) {
      try {
        keys.add(MinioCalls.call(result::get).objectName());
      } catch (NoSuchFileException e) {
        return;
      }
    }
    removeObjects(keys);
  }

  private void removeObjects(List<String> keys) throws IOException {
    for (int from = 0; from < keys.size(); from += MAX_DELETE_BATCH) {
      List<DeleteObject> batch = new ArrayList<>();
      for (String key : keys.subList(from, Math.min(keys.size(), from + MAX_DELETE_BATCH))) {
        batch.add(new DeleteObject(key));
      }
      Iterable<Result<DeleteError>> errors =
          client.removeObjects(RemoveObjectsArgs.builder().bucket(bucket).objects(batch).build());
      for (Result<DeleteError> error : errors) {
        DeleteError deleteError = MinioCalls.call(error::get);
        throw new IOException(
            "failed to delete " + deleteError.objectName() + ": " + deleteError.message());
      }
    }
  }

  private Object lockOf(String fullName) {
    return locks.computeIfAbsent(fullName, k -> new Object());
  }

  private <T> List<T> invokeAll(List<Callable<T>> tasks) throws IOException {
    if (tasks.size() == 1) {
      try {
        return Collections.singletonList(tasks.get(0).call());
      } catch (IOException | RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    List<Future<T>> futures = new ArrayList<>();
    for (Callable<T> task : tasks) {
      futures.add(executor.submit(task));
    }
    List<T> results = new ArrayList<>();
    IOException failure = null;
    for (Future<T> future : futures) {
      try {
        results.add(future.get());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("interrupted", e);
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        IOException exception =
            cause instanceof IOException ? (IOException) cause : new IOException(cause);
        if (failure == null) {
          failure = exception;
        } else {
          failure.addSuppressed(exception);
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
    return results;
  }

  @Override
  public void close() {}
}
