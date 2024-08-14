package cn.edu.tsinghua.iginx.filestore.struct.tree;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.filestore.common.IginxPaths;
import cn.edu.tsinghua.iginx.filestore.common.RowStreams;
import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;
import cn.edu.tsinghua.iginx.filestore.struct.FileManager;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Querier;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.ftj.FormatTreeJoin;
import cn.edu.tsinghua.iginx.filestore.thrift.DataBoundary;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

public class FileTreeManager implements FileManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileTreeManager.class);

  private final Path path;
  private final FileTreeConfig config;
  private final Querier.Builder builder;
  private final String prefix;

  public FileTreeManager(Path path, FileTreeConfig config) throws IOException {
    this.path = Objects.requireNonNull(path).toAbsolutePath();
    if (Objects.isNull(path.getFileName())) {
      this.config = config.withFilenameAsPrefix(false);
    } else {
      this.config = config;
    }
    this.prefix = config.isFilenameAsPrefix() ? IginxPaths.get(path.getFileName(), config.getDot()) : null;
    this.builder = new FormatTreeJoin().create(prefix, path, config);
  }

  @Override
  public DataBoundary getBoundary(@Nullable String requirePrefix) throws IOException {
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
    String prefix;
    Path afterPrefix;

    if (requiredPrefix == null || requiredPrefix.isEmpty()) {
      prefix = this.prefix;
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
  private Map.Entry<String, String> getColumnsInterval(Path path) throws IOException {
    if (Files.isRegularFile(path)) {
      return new AbstractMap.SimpleImmutableEntry<>(null, null);
    }

    try (Stream<Path> childStreamForMin = Files.list(path);
         Stream<Path> childStreamForMax = Files.list(path)) {
      Path minChild = childStreamForMin.min(Comparator.naturalOrder()).orElse(null);
      Path maxChild = childStreamForMax.max(Comparator.naturalOrder()).orElse(null);
      if (minChild == null || maxChild == null) {
        return null;
      }
      String startColumn = IginxPaths.get(minChild.getFileName(), config.getDot());
      String endColumn = IginxPaths.get(maxChild.getFileName(), config.getDot());
      return new AbstractMap.SimpleImmutableEntry<>(startColumn, endColumn);
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
    try (Querier querier = builder.build(target)) {
      List<RowStream> streams = querier.query();
      try {
        return RowStreams.merged(streams);
      } catch (PhysicalException e) {
        throw new IOException(e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    builder.close();
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
