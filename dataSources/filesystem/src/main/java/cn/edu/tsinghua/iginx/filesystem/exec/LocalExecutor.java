package cn.edu.tsinghua.iginx.filesystem.exec;

import static cn.edu.tsinghua.iginx.engine.logical.utils.ExprUtils.getKeyRangesFromFilter;
import static cn.edu.tsinghua.iginx.filesystem.shared.Constant.SEPARATOR;
import static cn.edu.tsinghua.iginx.filesystem.shared.Constant.WILDCARD;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream.EmptyRowStream;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.ColumnDataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.file.entity.FileMeta;
import cn.edu.tsinghua.iginx.filesystem.query.entity.FileSystemHistoryQueryRowStream;
import cn.edu.tsinghua.iginx.filesystem.query.entity.FileSystemQueryRowStream;
import cn.edu.tsinghua.iginx.filesystem.query.entity.FileSystemResultTable;
import cn.edu.tsinghua.iginx.filesystem.query.entity.Record;
import cn.edu.tsinghua.iginx.filesystem.shared.Constant;
import cn.edu.tsinghua.iginx.filesystem.shared.FileType;
import cn.edu.tsinghua.iginx.filesystem.tools.FilePathUtils;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalExecutor implements Executor {

  private static final Logger logger = LoggerFactory.getLogger(LocalExecutor.class);

  private String root;

  private boolean hasData;

  private FileSystemManager fileSystemManager;

  public LocalExecutor(boolean hasData, Map<String, String> extraParams) {
    String path = extraParams.getOrDefault(Constant.INIT_INFO_DIR, "/path/to/your/filesystem");
    File file = new File(path);
    if (file.isFile()) {
      logger.error("invalid directory: {}", file.getAbsolutePath());
      return;
    }
    this.root = file.getAbsolutePath() + SEPARATOR;
    this.hasData = hasData;
    this.fileSystemManager = new FileSystemManager(extraParams);
  }

  @Override
  public TaskExecuteResult executeProjectTask(
      List<String> paths,
      TagFilter tagFilter,
      Filter filter,
      String storageUnit,
      boolean isDummyStorageUnit) {
    if (isDummyStorageUnit) {
      if (tagFilter != null) {
        logger.error("dummy storage query should not contain tag filter");
        return new TaskExecuteResult(new EmptyRowStream());
      }
      return executeDummyProjectTask(paths, filter);
    }
    return executeQueryTask(storageUnit, paths, tagFilter, filter);
  }

  public TaskExecuteResult executeQueryTask(
      String storageUnit, List<String> paths, TagFilter tagFilter, Filter filter) {
    try {
      List<FileSystemResultTable> result = new ArrayList<>();
      logger.info("[Query] execute query file: {}", paths);
      List<KeyRange> keyRanges = getKeyRangesFromFilter(filter);
      for (String path : paths) {
        result.addAll(
            fileSystemManager.readFile(
                new File(FilePathUtils.toIginxPath(root, storageUnit, path)),
                tagFilter,
                keyRanges,
                false));
      }
      RowStream rowStream = new FileSystemQueryRowStream(result, storageUnit, root, filter);
      return new TaskExecuteResult(rowStream);
    } catch (Exception e) {
      logger.error(
          "read file error, storageUnit {}, paths({}), tagFilter({}), filter({})",
          storageUnit,
          paths,
          tagFilter,
          filter);
      e.printStackTrace();
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException("execute project task in fileSystem failure", e));
    }
  }

  public TaskExecuteResult executeDummyProjectTask(List<String> paths, Filter filter) {
    try {
      List<FileSystemResultTable> result = new ArrayList<>();
      logger.info("[Query] execute dummy query file: {}", paths);
      List<KeyRange> keyRanges = getKeyRangesFromFilter(filter);
      for (String path : paths) {
        result.addAll(
            fileSystemManager.readFile(
                new File(FilePathUtils.toNormalFilePath(root, path)), null, keyRanges, true));
      }
      RowStream rowStream =
          new FileSystemHistoryQueryRowStream(
              result, root, filter, fileSystemManager.getMemoryPool());
      return new TaskExecuteResult(rowStream);
    } catch (Exception e) {
      logger.error("read file error, paths {} filter {}", paths, filter);
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException(
              "execute dummy project task in fileSystem failure", e));
    }
  }

  @Override
  public TaskExecuteResult executeInsertTask(DataView dataView, String storageUnit) {
    Exception e = null;
    switch (dataView.getRawDataType()) {
      case Row:
      case NonAlignedRow:
        e = insertRowRecords((RowDataView) dataView, storageUnit);
        break;
      case Column:
      case NonAlignedColumn:
        e = insertColumnRecords((ColumnDataView) dataView, storageUnit);
        break;
    }
    if (e != null) {
      return new TaskExecuteResult(
          null, new PhysicalException("execute insert task in fileSystem failure", e));
    }
    return new TaskExecuteResult(null, null);
  }

  private Exception insertRowRecords(RowDataView data, String storageUnit) {
    List<List<Record>> recordsList = new ArrayList<>();
    List<File> fileList = new ArrayList<>();
    List<Map<String, String>> tagsList = new ArrayList<>();

    for (int j = 0; j < data.getPathNum(); j++) {
      fileList.add(new File(FilePathUtils.toIginxPath(root, storageUnit, data.getPath(j))));
      tagsList.add(data.getTags(j));
    }

    for (int j = 0; j < data.getPathNum(); j++) {
      recordsList.add(new ArrayList<>());
    }

    for (int i = 0; i < data.getKeySize(); i++) {
      BitmapView bitmapView = data.getBitmapView(i);
      int index = 0;
      for (int j = 0; j < data.getPathNum(); j++) {
        if (bitmapView.get(j)) {
          DataType dataType = data.getDataType(j);
          recordsList.get(j).add(new Record(data.getKey(i), dataType, data.getValue(i, index)));
          index++;
        }
      }
    }
    try {
      logger.info("begin to write data");
      return fileSystemManager.writeFiles(fileList, recordsList, tagsList);
    } catch (Exception e) {
      logger.error("encounter error when inserting row records to fileSystem: {}", e.getMessage());
    }
    return null;
  }

  private Exception insertColumnRecords(ColumnDataView data, String storageUnit) {
    List<List<Record>> recordsList = new ArrayList<>();
    List<File> fileList = new ArrayList<>();
    List<Map<String, String>> tagsList = new ArrayList<>();

    for (int j = 0; j < data.getPathNum(); j++) {
      fileList.add(new File(FilePathUtils.toIginxPath(root, storageUnit, data.getPath(j))));
      tagsList.add(data.getTags(j));
    }

    for (int i = 0; i < data.getPathNum(); i++) {
      List<Record> records = new ArrayList<>();
      BitmapView bitmapView = data.getBitmapView(i);
      DataType dataType = data.getDataType(i);
      int index = 0;
      for (int j = 0; j < data.getKeySize(); j++) {
        if (bitmapView.get(j)) {
          records.add(new Record(data.getKey(j), dataType, data.getValue(i, index)));
          index++;
        }
      }
      recordsList.add(records);
    }

    try {
      logger.info("begin to write data");
      return fileSystemManager.writeFiles(fileList, recordsList, tagsList);
    } catch (Exception e) {
      logger.error(
          "encounter error when inserting column records to fileSystem: {}", e.getMessage());
    }
    return null;
  }

  @Override
  public TaskExecuteResult executeDeleteTask(
      List<String> paths, List<KeyRange> keyRanges, TagFilter tagFilter, String storageUnit) {
    Exception exception = null;
    List<File> fileList = new ArrayList<>();
    if (keyRanges == null || keyRanges.isEmpty()) {
      if (paths.size() == 1 && paths.get(0).equals(WILDCARD) && tagFilter == null) {
        try {
          exception =
              fileSystemManager.deleteFile(
                  new File(FilePathUtils.toIginxPath(root, storageUnit, null)));
        } catch (Exception e) {
          logger.error("encounter error when clearing data: {}", e.getMessage());
          exception = e;
        }
      } else {
        for (String path : paths) {
          fileList.add(new File(FilePathUtils.toIginxPath(root, storageUnit, path)));
        }
        try {
          exception = fileSystemManager.deleteFiles(fileList, tagFilter);
        } catch (Exception e) {
          logger.error("encounter error when clearing data: {}", e.getMessage());
          exception = e;
        }
      }
    } else {
      try {
        if (!paths.isEmpty()) {
          for (String path : paths) {
            fileList.add(new File(FilePathUtils.toIginxPath(root, storageUnit, path)));
          }
          for (KeyRange keyRange : keyRanges) {
            exception =
                fileSystemManager.trimFilesContent(
                    fileList, tagFilter, keyRange.getActualBeginKey(), keyRange.getActualEndKey());
          }
        }
      } catch (IOException e) {
        logger.error("encounter error when deleting data: {}", e.getMessage());
        exception = e;
      }
    }
    return new TaskExecuteResult(null, exception != null ? new PhysicalException(exception) : null);
  }

  @Override
  public List<Column> getColumnsOfStorageUnit(String storageUnit) throws PhysicalException {
    List<Column> columns = new ArrayList<>();
    File directory = new File(FilePathUtils.toIginxPath(root, storageUnit, null));
    List<File> files = fileSystemManager.getAllFilesWithoutDir(directory);

    for (File file : files) {
      // 如果加入该Storage时有数据，才读取该文件夹下的文件
      if (hasData && fileSystemManager.getFileType(file).equals(FileType.NORMAL_FILE)) {
        columns.add(
            new Column(
                FilePathUtils.convertAbsolutePathToPath(root, file.getAbsolutePath(), storageUnit),
                DataType.BINARY,
                null));
      } else if (fileSystemManager.getFileType(file).equals(FileType.IGINX_FILE)) {
        FileMeta meta = fileSystemManager.getFileMeta(file);
        if (meta == null) {
          throw new PhysicalException(
              "encounter error when getting columns of storage unit: "
                  + file.getAbsolutePath()
                  + ", meta is null");
        }
        columns.add(
            new Column(
                FilePathUtils.convertAbsolutePathToPath(root, file.getAbsolutePath(), storageUnit),
                meta.getDataType(),
                meta.getTags()));
      }
    }

    return columns;
  }

  @Override
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(String dataPrefix)
      throws PhysicalException {
    KeyInterval keyInterval = new KeyInterval(0, Long.MAX_VALUE);
    ColumnsInterval columnsInterval;

    File directory = new File(FilePathUtils.toNormalFilePath(root, dataPrefix));
    if (dataPrefix != null && !dataPrefix.isEmpty()) {
      columnsInterval = new ColumnsInterval(dataPrefix);
    } else {
      Pair<File, File> filePair = fileSystemManager.getBoundaryOfFiles(directory);
      if (filePair == null) {
        columnsInterval = new ColumnsInterval(null, null);
      } else {
        columnsInterval =
            new ColumnsInterval(
                FilePathUtils.convertAbsolutePathToPath(root, filePair.k.getAbsolutePath(), null),
                FilePathUtils.convertAbsolutePathToPath(root, filePair.v.getAbsolutePath(), null));
      }
    }

    return new Pair<>(columnsInterval, keyInterval);
  }

  @Override
  public void close() {
    fileSystemManager.close();
  }
}
