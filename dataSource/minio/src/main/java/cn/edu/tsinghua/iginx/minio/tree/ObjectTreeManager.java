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
package cn.edu.tsinghua.iginx.minio.tree;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.minio.common.Filters;
import cn.edu.tsinghua.iginx.minio.common.IginxPaths;
import cn.edu.tsinghua.iginx.minio.common.KeyRanges;
import cn.edu.tsinghua.iginx.minio.common.Patterns;
import cn.edu.tsinghua.iginx.minio.common.RowStreams;
import cn.edu.tsinghua.iginx.minio.struct.DataBoundary;
import cn.edu.tsinghua.iginx.minio.struct.DataTarget;
import cn.edu.tsinghua.iginx.minio.struct.ObjectManager;
import cn.edu.tsinghua.iginx.minio.tree.format.ObjectFormat;
import cn.edu.tsinghua.iginx.minio.tree.format.ObjectFormats;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import com.google.common.collect.RangeSet;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads objects under a path as a tree of columns (the FileTree structure): a directory {@code d}
 * is the column prefix {@code d}, and an object is parsed by the format of its extension, with its
 * name as the prefix of its columns. Dots in names are written as {@code dot} in column names.
 *
 * <p>Objects matching the patterns are read in parallel and merged by key, then the filter is
 * applied to merged rows. Tags are not supported, the tag filter is ignored.
 */
public class ObjectTreeManager implements ObjectManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ObjectTreeManager.class);

  private final Path root;
  private final ObjectTreeConfig config;
  private final ExecutorService executor;

  public ObjectTreeManager(Path root, ObjectTreeConfig config, ExecutorService executor)
      throws IOException {
    this.root = Objects.requireNonNull(root).normalize();
    this.config = Objects.requireNonNull(config);
    this.executor = Objects.requireNonNull(executor);
    if (!Files.isDirectory(this.root) && !Files.isRegularFile(this.root)) {
      throw new NoSuchFileException(this.root.toString());
    }
    LOGGER.debug("create object tree manager on {} with {}", this.root, config);
  }

  @Override
  public DataBoundary getBoundary(@Nullable String requiredPrefix) throws IOException {
    String prefix = config.getPrefix();
    if (config.getBoundaryLevel() == 0 && prefix != null) {
      if (requiredPrefix == null || requiredPrefix.startsWith(prefix)) {
        return DataBoundary.of(prefix, StringUtils.nextString(prefix));
      }
      return DataBoundary.empty();
    }

    String embedded = IginxPaths.toStringPrefix(prefix);
    String required = IginxPaths.toStringPrefix(requiredPrefix);
    int common = 0;
    while (common < Math.min(embedded.length(), required.length())
        && embedded.charAt(common) == required.charAt(common)) {
      common++;
    }
    if (common < Math.min(embedded.length(), required.length())) {
      LOGGER.warn("prefix mismatch: {} vs {}", embedded, required);
      return DataBoundary.empty();
    }
    String requiredRest = IginxPaths.fromStringPrefix(required.substring(common));
    String targetPrefix = IginxPaths.fromStringPrefix(embedded + required.substring(common));
    Path target =
        root.resolve(IginxPaths.toFilePath(requiredRest, config.getDot(), root.getFileSystem()));

    if (Files.isRegularFile(target)) {
      return DataBoundary.of(
          targetPrefix, targetPrefix == null ? null : StringUtils.nextString(targetPrefix));
    }
    String min;
    String max;
    try (Stream<Path> forMin = Files.list(target);
        Stream<Path> forMax = Files.list(target)) {
      min = forMin.map(this::nameOf).min(Comparator.naturalOrder()).orElse(null);
      max = forMax.map(this::nameOf).max(Comparator.naturalOrder()).orElse(null);
    } catch (NoSuchFileException e) {
      LOGGER.warn("directory does not exist: {}", target);
      return DataBoundary.empty();
    }
    if (min == null || max == null) {
      return DataBoundary.empty();
    }
    String end = IginxPaths.join(targetPrefix, StringUtils.nextString(max));
    return DataBoundary.of(
        IginxPaths.join(targetPrefix, min), end == null ? null : StringUtils.nextString(end));
  }

  private String nameOf(Path path) {
    return IginxPaths.get(path.getFileName(), config.getDot());
  }

  @Override
  public RowStream query(DataTarget target) throws IOException {
    List<Future<RowStream>> futures = new ArrayList<>();
    try {
      collect(config.getPrefix(), root, target.getPatterns(), target, futures);
    } catch (IOException | RuntimeException e) {
      futures.forEach(future -> future.cancel(true));
      throw e;
    }

    List<RowStream> streams = new ArrayList<>();
    Exception failure = null;
    for (Future<RowStream> future : futures) {
      try {
        streams.add(future.get());
      } catch (ExecutionException e) {
        failure =
            addSuppressed(
                failure, e.getCause() instanceof Exception ? (Exception) e.getCause() : e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        failure = addSuppressed(failure, e);
      }
    }
    try {
      if (failure != null) {
        throw failure;
      }
      return RowStreams.filtered(RowStreams.union(streams), target.getFilter());
    } catch (Exception e) {
      try {
        RowStreams.close(streams);
      } catch (PhysicalException closeFailure) {
        e.addSuppressed(closeFailure);
      }
      if (e instanceof IOException) {
        throw (IOException) e;
      }
      throw new IOException("failed to query " + root + ": " + e, e);
    }
  }

  private static Exception addSuppressed(@Nullable Exception first, Exception e) {
    if (first == null) {
      return e;
    }
    first.addSuppressed(e);
    return first;
  }

  /** submit reading of objects under path whose columns may match patterns */
  private void collect(
      @Nullable String prefix,
      Path path,
      @Nullable List<String> patterns,
      DataTarget target,
      List<Future<RowStream>> futures)
      throws IOException {
    if (Files.isDirectory(path)) {
      for (Map.Entry<String, Path> child : matchedChildren(prefix, path, patterns).entrySet()) {
        String childPrefix = child.getKey();
        collect(
            childPrefix,
            child.getValue(),
            Patterns.filterByPrefix(patterns, childPrefix),
            target,
            futures);
      }
    } else if (Files.isRegularFile(path)) {
      List<String> objectPatterns = Patterns.nullToAll(patterns);
      futures.add(executor.submit(() -> readObject(prefix, path, objectPatterns, target)));
    } else if (!Files.exists(path)) {
      throw new NoSuchFileException(path.toString());
    }
  }

  /** children named by the next node of patterns, all children if the node is a wildcard */
  private Map<String, Path> matchedChildren(
      @Nullable String prefix, Path dir, @Nullable List<String> patterns) throws IOException {
    Map<String, Path> children = new LinkedHashMap<>();
    for (String pattern : Patterns.nullToAll(patterns)) {
      if (!Patterns.startsWith(pattern, prefix)) {
        continue;
      }
      String[] nodes = IginxPaths.split(Patterns.suffix(pattern, prefix));
      if (nodes.length == 0) {
        continue;
      }
      if (Patterns.isWildcard(nodes[0])) {
        return allChildren(prefix, dir);
      }
      Path child =
          dir.resolve(IginxPaths.toFilePath(nodes[0], config.getDot(), dir.getFileSystem()));
      if (Files.exists(child)) {
        children.put(IginxPaths.join(prefix, nameOf(child)), child);
      }
    }
    return children;
  }

  private Map<String, Path> allChildren(@Nullable String prefix, Path dir) throws IOException {
    Map<String, Path> children = new LinkedHashMap<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
      for (Path child : stream) {
        children.put(IginxPaths.join(prefix, nameOf(child)), child);
      }
    }
    return children;
  }

  private RowStream readObject(
      @Nullable String prefix, Path path, List<String> patterns, DataTarget target)
      throws IOException {
    ObjectFormat format = ObjectFormats.of(path);
    ObjectFormat.Reader reader =
        format.newReader(prefix, path, config.getFormatConfig(format.getName()));
    Map<String, DataType> schema = reader.find(patterns);
    if (schema.isEmpty()) {
      return RowStreams.empty();
    }
    RangeSet<Long> keys = KeyRanges.of(target.getFilter());
    if (Filters.isFalse(target.getFilter()) || keys.isEmpty()) {
      List<Field> fields = new ArrayList<>();
      schema.forEach((name, type) -> fields.add(new Field(name, type)));
      return RowStreams.empty(new Header(Field.KEY, fields));
    }
    return reader.read(new ArrayList<>(schema.keySet()), keys);
  }

  @Override
  public void delete(DataTarget target) {
    throw new UnsupportedOperationException("FileTree is read only");
  }

  @Override
  public void insert(DataView data) {
    throw new UnsupportedOperationException("FileTree is read only");
  }

  @Override
  public void close() {}
}
