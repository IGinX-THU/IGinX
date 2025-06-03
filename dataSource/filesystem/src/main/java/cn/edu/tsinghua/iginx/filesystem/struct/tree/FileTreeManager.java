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
package cn.edu.tsinghua.iginx.filesystem.struct.tree;

import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.filesystem.common.IginxPaths;
import cn.edu.tsinghua.iginx.filesystem.struct.DataTarget;
import cn.edu.tsinghua.iginx.filesystem.struct.FileManager;
import cn.edu.tsinghua.iginx.filesystem.struct.tree.query.Querier;
import cn.edu.tsinghua.iginx.filesystem.struct.tree.query.Queriers;
import cn.edu.tsinghua.iginx.filesystem.struct.tree.query.ftj.UnionFormatTree;
import cn.edu.tsinghua.iginx.filesystem.thrift.DataBoundary;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileTreeManager implements FileManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileTreeManager.class);

  private final Path path;
  private final FileTreeConfig config;
  private final Querier.Builder builder;
  private final ExecutorService executor;

  public FileTreeManager(Path path, FileTreeConfig config) throws IOException {
    LOGGER.debug("Create Manager in {} with {}", path, config);
    this.path = Objects.requireNonNull(path).normalize();
    this.config = config;
    this.executor =
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder().setNameFormat("FileTreeManager-%d").build());
    this.builder = new UnionFormatTree().create(config.getPrefix(), path, config, executor);
  }

  @Override
  public DataBoundary getBoundary(@Nullable String requirePrefix) throws IOException {
    LOGGER.debug("Getting boundary for {} with prefix {}", path, requirePrefix);

    DataBoundary boundary = new DataBoundary();

    Map.Entry<String, Path> targetPrefixAndPath = getTargetPrefixAndPath(requirePrefix);
    if (targetPrefixAndPath != null) {
      String prefix = targetPrefixAndPath.getKey();
      Path afterPrefix = targetPrefixAndPath.getValue();

      Map.Entry<String, String> columnsInterval = getColumnsInterval(afterPrefix);
      if (columnsInterval != null) {
        boundary.setStartColumn(IginxPaths.join(prefix, columnsInterval.getKey()));
        String endColumn = IginxPaths.join(prefix, columnsInterval.getValue());
        boundary.setEndColumn(endColumn);
        if (endColumn != null) {
          boundary.setEndColumn(StringUtils.nextString(endColumn));
        }
        boundary.setStartKey(KeyInterval.getDefaultKeyInterval().getStartKey());
        boundary.setEndKey(KeyInterval.getDefaultKeyInterval().getEndKey());
      }
    }

    return boundary;
  }

  @Nullable
  private Map.Entry<String, Path> getTargetPrefixAndPath(@Nullable String requiredPrefix) {
    String embeddedStringPrefix = IginxPaths.toStringPrefix(config.getPrefix());
    String requiredStringPrefix = IginxPaths.toStringPrefix(requiredPrefix);
    String commonStringPrefix = Strings.commonPrefix(embeddedStringPrefix, requiredStringPrefix);

    if (commonStringPrefix.length()
        < Math.min(embeddedStringPrefix.length(), requiredStringPrefix.length())) {
      LOGGER.warn("Prefix mismatch: {} vs {}", embeddedStringPrefix, requiredStringPrefix);
      return null;
    }

    String requiredStringPrefixWithoutCommon =
        requiredStringPrefix.substring(commonStringPrefix.length());
    String requiredPrefixWithoutCommon =
        IginxPaths.fromStringPrefix(requiredStringPrefixWithoutCommon);

    String targetPrefix =
        IginxPaths.fromStringPrefix(embeddedStringPrefix + requiredStringPrefixWithoutCommon);
    Path afterPrefix =
        path.resolve(
            IginxPaths.toFilePath(
                requiredPrefixWithoutCommon, config.getDot(), path.getFileSystem()));

    return new AbstractMap.SimpleImmutableEntry<>(targetPrefix, afterPrefix);
  }

  @Nullable
  private Map.Entry<String, String> getColumnsInterval(Path path) throws IOException {
    if (Files.isRegularFile(path)) {
      LOGGER.info("Path is a file: {}", path);
      return new AbstractMap.SimpleImmutableEntry<>(null, null);
    }

    try (Stream<Path> childStreamForMin = Files.list(path);
        Stream<Path> childStreamForMax = Files.list(path)) {
      String minChild =
          childStreamForMin
              .map(Path::getFileName)
              .map(p -> IginxPaths.get(p, config.getDot()))
              .min(Comparator.naturalOrder())
              .orElse(null);
      String maxChild =
          childStreamForMax
              .map(Path::getFileName)
              .map(p -> IginxPaths.get(p, config.getDot()))
              .max(Comparator.naturalOrder())
              .orElse(null);

      if (minChild == null || maxChild == null) {
        return null;
      }

      LOGGER.debug("Start column: {}", minChild);
      LOGGER.debug("End column: {}", maxChild);
      return new AbstractMap.SimpleImmutableEntry<>(minChild, StringUtils.nextString(maxChild));
    } catch (NoSuchFileException e) {
      LOGGER.warn("Directory does not exist: {}", path, e);
      return null;
    }
  }

  @Override
  public RowStream query(DataTarget target, @Nullable AggregateType aggregate) throws IOException {
    if (aggregate != null) {
      throw new UnsupportedOperationException("Aggregate not supported");
    }
    LOGGER.debug("Querying {} ", target);
    try (Querier querier = builder.build(target)) {
      LOGGER.debug("Querier is built as: {}", querier);
      List<Future<RowStream>> streams = querier.query();
      Future<RowStream> union = Queriers.union(streams);
      try {
        return union.get();
      } catch (ExecutionException | InterruptedException e) {
        throw new IOException(e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    builder.close();
    executor.shutdown();
  }

  @Override
  public void delete(DataTarget target) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void insert(DataView data) throws IOException {
    throw new UnsupportedOperationException();
  }
}
