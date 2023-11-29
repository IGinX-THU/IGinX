package cn.edu.tsinghua.iginx.parquet.exec;

import static cn.edu.tsinghua.iginx.parquet.entity.Constants.SUFFIX_PARQUET_FILE;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.parquet.entity.Field;
import cn.edu.tsinghua.iginx.parquet.entity.NewQueryRowStream;
import cn.edu.tsinghua.iginx.parquet.entity.Table;
import cn.edu.tsinghua.iginx.parquet.io.parquet.Loader;
import cn.edu.tsinghua.iginx.parquet.tools.TagKVUtils;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyManager implements Manager {

  private static final Logger logger = LoggerFactory.getLogger(DummyManager.class);

  private final Path dir;

  private final String prefix;

  public DummyManager(@NotNull Path dummyDir, @NotNull String prefix) {
    this.dir = dummyDir;
    this.prefix = prefix;
  }

  @Override
  public RowStream project(List<String> paths, TagFilter tagFilter, Filter filter)
      throws PhysicalException {
    Table table = new Table();
    for (Path path : getFilePaths()) {
      Set<String> pathsInFile;
      try {
        pathsInFile =
            new Loader(path)
                .getHeader().stream()
                    .map(Field::getName)
                    .map(s -> prefix + "." + s)
                    .collect(Collectors.toSet());
      } catch (IOException e) {
        throw new PhysicalException("failed to load schema from " + path, e);
      }

      List<String> filePaths = determinePathList(pathsInFile, paths, tagFilter);
      filePaths.replaceAll(s -> s.substring(s.indexOf(".") + 1));
      if (!filePaths.isEmpty()) {
        // TODO: filter, project
        try {
          new Loader(path).load(table);
        } catch (IOException e) {
          throw new PhysicalException("failed to load data from " + path, e);
        }
      }
    }
    List<cn.edu.tsinghua.iginx.parquet.entity.Column> columns =
        table.toColumns().stream()
            .filter(column -> paths.contains(column.getPathName()))
            .collect(Collectors.toList());
    columns.forEach(
        column -> {
          column.setPathName(prefix + "." + column.getPathName());
        });
    return new NewQueryRowStream(columns);
  }

  private List<String> determinePathList(
      Set<String> paths, List<String> patterns, TagFilter tagFilter) {
    List<String> ret = new ArrayList<>();
    for (String path : paths) {
      for (String pattern : patterns) {
        Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(path);
        if (tagFilter == null) {
          if (Pattern.matches(StringUtils.reformatPath(pattern), pair.getK())) {
            ret.add(path);
            break;
          }
        } else {
          if (Pattern.matches(StringUtils.reformatPath(pattern), pair.getK())
              && TagKVUtils.match(pair.getV(), tagFilter)) {
            ret.add(path);
            break;
          }
        }
      }
    }
    return ret;
  }

  @Override
  public void insert(DataView dataView) throws PhysicalException {
    throw new PhysicalException("DummyManager does not support insert");
  }

  @Override
  public void delete(List<String> paths, List<KeyRange> keyRanges, TagFilter tagFilter)
      throws PhysicalException {
    throw new PhysicalException("DummyManager does not support delete");
  }

  @Override
  public List<Column> getColumns() throws PhysicalException {
    List<Column> columns = new ArrayList<>();
    for (Path path : getFilePaths()) {
      try {
        List<Field> fields = new Loader(path).getHeader();
        for (Field field : fields) {
          Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(field.getName());
          Column column = new Column(prefix + "." + pair.k, field.getType(), pair.v, true);
          columns.add(column);
        }
      } catch (IOException e) {
        throw new PhysicalException("failed to load schema from " + path, e);
      }
    }
    return columns;
  }

  @Override
  public KeyInterval getKeyInterval() throws PhysicalException {
    // TODO: get key interval from parquet files
    return new KeyInterval(0, Long.MAX_VALUE);
  }

  @Override
  public void close() throws IOException {
    logger.info("{} closed", this);
  }

  @Override
  public String toString() {
    return "DummyManager{" + "dummyDir=" + dir + '}';
  }

  private Iterable<Path> getFilePaths() throws PhysicalException {
    try (Stream<Path> pathStream = Files.list(dir)) {
      return pathStream
          .filter(path -> path.toString().endsWith(SUFFIX_PARQUET_FILE))
          .filter(Files::isRegularFile)
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new PhysicalException("failed to list parquet file in " + dir, e);
    }
  }
}
