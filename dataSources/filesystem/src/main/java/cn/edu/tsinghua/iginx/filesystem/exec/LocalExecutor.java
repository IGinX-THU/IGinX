package cn.edu.tsinghua.iginx.filesystem.exec;

import static cn.edu.tsinghua.iginx.engine.logical.utils.ExprUtils.getKeyRangesFromFilter;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
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
import cn.edu.tsinghua.iginx.filesystem.file.type.FileType;
import cn.edu.tsinghua.iginx.filesystem.query.entity.FileSystemHistoryQueryRowStream;
import cn.edu.tsinghua.iginx.filesystem.query.entity.FileSystemQueryRowStream;
import cn.edu.tsinghua.iginx.filesystem.query.entity.FileSystemResultTable;
import cn.edu.tsinghua.iginx.filesystem.query.entity.Record;
import cn.edu.tsinghua.iginx.filesystem.tools.FilePathUtils;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
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

  public LocalExecutor() {
    this(null, false);
  }

  public LocalExecutor(String root, boolean hasData) {
    if (root == null) {
      logger.error("root should not be null!");
    } else {
      this.hasData = hasData;
      this.root = root;
    }
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
        return new TaskExecuteResult(new FileSystemHistoryQueryRowStream());
      }
      return executeDummyProjectTask(paths, filter);
    }
    return executeQueryTask(storageUnit, paths, tagFilter, filter);
  }

  public TaskExecuteResult executeQueryTask(
      String storageUnit, List<String> paths, TagFilter tagFilter, Filter filter) {
    try {
      List<FileSystemResultTable> result = new ArrayList<>();
      logger.info("[Query] execute query file: " + paths);
      List<KeyRange> keyRanges = getKeyRangesFromFilter(filter);
      for (String path : paths) {
        result.addAll(
            FileSystemManager.readFile(
                new File(FilePathUtils.toIginxPath(root, storageUnit, path)),
                tagFilter,
                keyRanges,
                false));
      }
      RowStream rowStream = new FileSystemQueryRowStream(result, storageUnit, root, filter);
      return new TaskExecuteResult(rowStream);
    } catch (Exception e) {
      logger.error(
          String.format(
              "read file error, storageUnit %s, paths(%s), tagFilter(%s), filter(%s)",
              storageUnit, paths, tagFilter, filter));
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException("execute project task in fileSystem failure", e));
    }
  }

  public TaskExecuteResult executeDummyProjectTask(List<String> paths, Filter filter) {
    try {
      List<FileSystemResultTable> result = new ArrayList<>();
      logger.info("[Query] execute dummy query file: " + paths);
      List<KeyRange> keyRanges = getKeyRangesFromFilter(filter);
      for (String path : paths) {
        result.addAll(
            FileSystemManager.readFile(
                new File(FilePathUtils.toNormalFilePath(root, path)), null, keyRanges, true));
      }
      RowStream rowStream = new FileSystemHistoryQueryRowStream(result, root, filter);
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
      logger.info("开始数据写入");
      return FileSystemManager.writeFiles(fileList, recordsList, tagsList);
    } catch (Exception e) {
      logger.error("encounter error when inserting row records to fileSystem: ", e);
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
      logger.info("开始数据写入");
      return FileSystemManager.writeFiles(fileList, recordsList, tagsList);
    } catch (Exception e) {
      logger.error("encounter error when inserting column records to fileSystem: ", e);
    }
    return null;
  }

  @Override
  public TaskExecuteResult executeDeleteTask(
      List<String> paths, List<KeyRange> keyRanges, TagFilter tagFilter, String storageUnit) {
    Exception exception = null;
    List<File> fileList = new ArrayList<>();
    if (keyRanges == null || keyRanges.isEmpty()) {
      if (paths.size() == 1 && paths.get(0).equals("*") && tagFilter == null) {
        try {
          exception =
              FileSystemManager.deleteFile(
                  new File(FilePathUtils.toIginxPath(root, storageUnit, null)));
        } catch (Exception e) {
          logger.error("encounter error when clear data: " + e.getMessage());
          exception = e;
        }
      } else {
        for (String path : paths) {
          fileList.add(new File(FilePathUtils.toIginxPath(root, storageUnit, path)));
        }
        try {
          exception = FileSystemManager.deleteFiles(fileList, tagFilter);
        } catch (Exception e) {
          logger.error("encounter error when clear data: " + e.getMessage());
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
                FileSystemManager.trimFilesContent(
                    fileList, tagFilter, keyRange.getActualBeginKey(), keyRange.getActualEndKey());
          }
        }
      } catch (IOException e) {
        logger.error("encounter error when delete data: " + e.getMessage());
        exception = e;
      }
    }
    return new TaskExecuteResult(null, exception != null ? new PhysicalException(exception) : null);
  }

  @Override
  public List<Column> getColumnsOfStorageUnit(String storageUnit) throws PhysicalException {
    List<Column> files = new ArrayList<>();

    File directory = new File(FilePathUtils.toIginxPath(root, storageUnit, null));

    List<File> allFiles = FileSystemManager.getAllFilesWithoutDir(directory);

    for (File file : allFiles) {
      try { // 如果加入该Storage时有数据，才读取该文件夹下的文件
        if (hasData && FileType.getFileType(file).equals(FileType.NORMAL_FILE)) {
          files.add(
              new Column(
                  FilePathUtils.convertAbsolutePathToPath(
                      root, file.getAbsolutePath(), storageUnit),
                  DataType.BINARY,
                  null));
        } else {
          FileMeta meta = FileSystemManager.getFileMeta(file);
          if (meta == null) {
            logger.error(
                "encounter error when get columns of storage unit: "
                    + file.getAbsolutePath()
                    + ", meta is null");
            throw new PhysicalException(
                "encounter error when get columns of storage unit: "
                    + file.getAbsolutePath()
                    + ", meta is null");
          }
          files.add(
              new Column(
                  FilePathUtils.convertAbsolutePathToPath(
                      root, file.getAbsolutePath(), storageUnit),
                  meta.getDataType(),
                  meta.getTags()));
        }
      } catch (IOException e) {
        logger.error("encounter error when get columns of storage unit: " + e.getMessage());
        throw new PhysicalException(e.getMessage());
      }
    }

    return files;
  }

  @Override
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(String prefix)
      throws PhysicalException {
    File directory = new File(FilePathUtils.toNormalFilePath(root, prefix));

    Pair<File, File> files = FileSystemManager.getBoundaryFiles(directory);

    if (files == null) {
      throw new PhysicalTaskExecuteFailureException("no data!");
    }

    File minPathFile = files.getK();
    File maxPathFile = files.getV();

    ColumnsInterval tsInterval = null;
    if (prefix == null)
      tsInterval =
          new ColumnsInterval(
              FilePathUtils.convertAbsolutePathToPath(root, minPathFile.getAbsolutePath(), null),
              StringUtils.nextString(
                  FilePathUtils.convertAbsolutePathToPath(
                      root, maxPathFile.getAbsolutePath(), null)));
    else tsInterval = new ColumnsInterval(prefix, StringUtils.nextString(prefix));

    // 对于pb级的文件系统，遍历是不可能的，直接接入
    Long time = FileSystemManager.getMaxTime(directory);
    KeyInterval keyInterval = new KeyInterval(0, time == Long.MIN_VALUE ? Long.MAX_VALUE : time);

    return new Pair<>(tsInterval, keyInterval);
  }

  @Override
  public void close() throws PhysicalException {}
}
