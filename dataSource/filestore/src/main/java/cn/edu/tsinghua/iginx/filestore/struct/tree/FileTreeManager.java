package cn.edu.tsinghua.iginx.filestore.struct.tree;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream.EmptyRowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.filestore.common.RowStreams;
import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;
import cn.edu.tsinghua.iginx.filestore.struct.FileManager;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.FormatTreeJoin;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Querier;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.QuerierBuilder;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.QuerierBuilderFactory;
import cn.edu.tsinghua.iginx.filestore.thrift.DataBoundary;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;
import javax.annotation.Nullable;

public class FileTreeManager implements FileManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileTreeManager.class);

  private final Path path;
  private final FileTreeConfig config;
  private final QuerierBuilder querierBuilder;

  public FileTreeManager(Path path, FileTreeConfig config) throws IOException {
    this.path = Objects.requireNonNull(path).toAbsolutePath();
    this.config = Objects.requireNonNull(config);
    if (config.isFilenameAsPrefix() && path.getFileName() == null) {
      throw new IllegalArgumentException("Path does not have a file name, but `filenameAsPrefix` is true");
    }
    String prefix = config.isFilenameAsPrefix() ? IginxPaths.get(path.getFileName(), config.getDot()) : null;
    QuerierBuilderFactory factory = new FormatTreeJoin();
    this.querierBuilder = factory.create(path, prefix, config);
  }

  @Override
  public DataBoundary getBoundary(@Nullable String requirePrefix) throws IOException {
    DataBoundary boundary = new DataBoundary();

    Map.Entry<String, Path> targetPrefixAndPath = getTargetPrefixAndPath(requirePrefix);
    if (targetPrefixAndPath != null) {
      String prefix = targetPrefixAndPath.getKey();
      Path afterPrefix = targetPrefixAndPath.getValue();

      ColumnsInterval interval = getColumnsInterval(afterPrefix);
      if (interval != null) {
        boundary.setStartColumn(IginxPaths.get(prefix, interval.getStartColumn()));
        boundary.setEndColumn(IginxPaths.get(prefix, interval.getEndColumn()));
        boundary.setStartKey(KeyInterval.getDefaultKeyInterval().getStartKey());
        boundary.setEndKey(KeyInterval.getDefaultKeyInterval().getEndKey());
      }
    }

    return boundary;
  }

  @Nullable
  private Map.Entry<String, Path> getTargetPrefixAndPath(@Nullable String requiredPrefix) {
    String prefix;
    Path afterPrefix;

    if (requiredPrefix == null || requiredPrefix.isEmpty()) {
      if (config.isFilenameAsPrefix()) {
        prefix = IginxPaths.get(path.getFileName(), config.getDot());
      } else {
        prefix = null;
      }
      afterPrefix = path;
    } else {
      Path prefixRelativePath = IginxPaths.toFilePath(requiredPrefix, config.getDot(), path.getFileSystem());

      if (prefixRelativePath.isAbsolute()) {
        return null;
      }
      if (!Objects.equals(prefixRelativePath, prefixRelativePath.normalize())) {
        return null;
      }

      prefix = IginxPaths.get(prefixRelativePath, config.getDot());
      if (config.isFilenameAsPrefix()) {
        if (!Objects.equals(prefixRelativePath.getName(0), path.getFileName())) {
          return null;
        }
        afterPrefix = path.resolveSibling(prefixRelativePath);
      } else {
        afterPrefix = path.resolve(prefixRelativePath);
      }
    }

    return new AbstractMap.SimpleImmutableEntry<>(prefix, afterPrefix);
  }

  @Nullable
  private static ColumnsInterval getColumnsInterval(Path path) throws IOException {
    if (Files.isRegularFile(path)) {
      return new ColumnsInterval(null, null);
    }

    try (Stream<Path> childStreamForMin = Files.list(path);
         Stream<Path> childStreamForMax = Files.list(path)) {
      Path minChild = childStreamForMin.min(Comparator.naturalOrder()).orElse(null);
      Path maxChild = childStreamForMax.max(Comparator.naturalOrder()).orElse(null);
      if (minChild == null || maxChild == null) {
        return null;
      }
      String startColumn = minChild.getFileName().toString();
      String endColumn = StringUtils.nextString(maxChild.getFileName().toString());
      return new ColumnsInterval(startColumn, endColumn);
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
    try (Querier querier = querierBuilder.build(target)) {
      List<RowStream> streams = querier.query();
      try {
        return RowStreams.merge(streams);
      } catch (PhysicalException e) {
        throw new IOException(e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    querierBuilder.close();
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
