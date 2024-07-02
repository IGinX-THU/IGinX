/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.filesystem.exec;

import static cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils.getKeyRangesFromFilter;
import static cn.edu.tsinghua.iginx.filesystem.shared.Constant.SEPARATOR;
import static cn.edu.tsinghua.iginx.filesystem.shared.Constant.WILDCARD;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
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
import cn.edu.tsinghua.iginx.filesystem.exception.FileSystemTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.filesystem.exception.FilesystemException;
import cn.edu.tsinghua.iginx.filesystem.file.entity.FileMeta;
import cn.edu.tsinghua.iginx.filesystem.query.entity.FileSystemHistoryQueryRowStream;
import cn.edu.tsinghua.iginx.filesystem.query.entity.FileSystemQueryRowStream;
import cn.edu.tsinghua.iginx.filesystem.query.entity.FileSystemResultTable;
import cn.edu.tsinghua.iginx.filesystem.query.entity.Record;
import cn.edu.tsinghua.iginx.filesystem.shared.Constant;
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

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalExecutor.class);

  private String root;

  private String dummyRoot;

  private String realDummyRoot;

  private String prefix;

  private boolean hasData;

  private FileSystemManager fileSystemManager;

  public LocalExecutor(boolean isReadOnly, boolean hasData, Map<String, String> extraParams) {
    String dir = extraParams.get(Constant.INIT_INFO_DIR);
    String dummyDir = extraParams.get(Constant.INIT_INFO_DUMMY_DIR);
    String prefix = extraParams.get(Constant.INIT_ROOT_PREFIX);
    try {
      if (hasData) {
        if (dummyDir == null || dummyDir.isEmpty()) {
          throw new IllegalArgumentException("No dummy_dir declared with params " + extraParams);
        }
        File dummyFile = new File(dummyDir);
        if (dummyFile.isFile()) {
          throw new IllegalArgumentException(String.format("invalid dummy_dir %s", dummyDir));
        }
        this.prefix = prefix;
        this.realDummyRoot = dummyFile.getCanonicalPath() + SEPARATOR;
        this.dummyRoot = dummyFile.getParentFile().getCanonicalPath() + SEPARATOR;
        if (!isReadOnly) {
          if (dir == null || dir.isEmpty()) {
            throw new IllegalArgumentException("No dir declared with params " + extraParams);
          }
          File file = new File(dir);
          if (file.isFile()) {
            throw new IllegalArgumentException(String.format("invalid dir %s", dir));
          }
          this.root = file.getCanonicalPath() + SEPARATOR;
          try {
            String dummyDirPath = dummyFile.getCanonicalPath();
            String dirPath = file.getCanonicalPath();
            if (dummyDirPath.equals(dirPath)) {
              throw new IllegalArgumentException(
                  String.format("dir %s cannot be equal to dummy directory %s", dir, dummyDir));
            }
          } catch (IOException e) {
            throw new IOException(
                String.format("get canonical path failed for dir %s dummy_dir %s", dir, dummyDir));
          }
        }
      } else {
        if (dir == null || dir.isEmpty()) {
          throw new IllegalArgumentException("No dir declared with params " + extraParams);
        }
        File file = new File(dir);
        if (file.isFile()) {
          throw new IllegalArgumentException(String.format("invalid dir %s", dir));
        }
        this.root = file.getCanonicalPath() + SEPARATOR;
      }
    } catch (IOException e) {
      LOGGER.error("get dir or dummy dir failure: ", e);
    }
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
        LOGGER.error("dummy storage query should not contain tag filter");
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
      LOGGER.info("[Query] execute query file: {}", paths);
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
      LOGGER.error(
          "read file error, storageUnit {}, paths({}), tagFilter({}), filter({})",
          storageUnit,
          paths,
          tagFilter,
          filter);
      return new TaskExecuteResult(
          new FileSystemTaskExecuteFailureException(
              String.format(
                  "read file error, storageUnit %s, paths(%s), tagFilter(%s), filter(%s)",
                  storageUnit, paths, tagFilter, filter),
              e));
    }
  }

  public TaskExecuteResult executeDummyProjectTask(List<String> paths, Filter filter) {
    try {
      List<FileSystemResultTable> result = new ArrayList<>();
      LOGGER.info("[Query] execute dummy query file: {}", paths);
      List<KeyRange> keyRanges = getKeyRangesFromFilter(filter);
      for (String path : paths) {
        result.addAll(
            fileSystemManager.readFile(
                new File(FilePathUtils.toNormalFilePath(dummyRoot, path)), null, keyRanges, true));
      }
      RowStream rowStream =
          new FileSystemHistoryQueryRowStream(
              result, dummyRoot, filter, fileSystemManager.getMemoryPool());
      return new TaskExecuteResult(rowStream);
    } catch (Exception e) {
      LOGGER.error("read file error, paths {} filter {}", paths, filter);
      return new TaskExecuteResult(
          new FileSystemTaskExecuteFailureException(
              String.format("read file error, paths %s filter %s", paths, filter), e));
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
          null, new FilesystemException("encounter error when inserting data: ", e));
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
      LOGGER.info("begin to write data");
      fileSystemManager.writeFiles(fileList, recordsList, tagsList);
    } catch (IOException e) {
      return new IOException("encounter error when inserting row records to fileSystem: ", e);
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
      LOGGER.info("begin to write data");
      fileSystemManager.writeFiles(fileList, recordsList, tagsList);
    } catch (IOException e) {
      return new IOException("encounter error when inserting column records to fileSystem: ", e);
    }
    return null;
  }

  @Override
  public TaskExecuteResult executeDeleteTask(
      List<String> paths, List<KeyRange> keyRanges, TagFilter tagFilter, String storageUnit) {
    List<File> fileList = new ArrayList<>();
    if (keyRanges == null || keyRanges.isEmpty()) {
      if (paths.size() == 1 && paths.get(0).equals(WILDCARD) && tagFilter == null) {
        try {
          fileSystemManager.deleteFile(
              new File(FilePathUtils.toIginxPath(root, storageUnit, null)));
        } catch (IOException e) {
          return new TaskExecuteResult(
              new FilesystemException("encounter error when clearing data: ", e));
        }
      } else {
        for (String path : paths) {
          fileList.add(new File(FilePathUtils.toIginxPath(root, storageUnit, path)));
        }
        try {
          fileSystemManager.deleteFiles(fileList, tagFilter);
        } catch (IOException e) {
          return new TaskExecuteResult(
              new FilesystemException("encounter error when clearing data: ", e));
        }
      }
    } else {
      try {
        if (!paths.isEmpty()) {
          for (String path : paths) {
            fileList.add(new File(FilePathUtils.toIginxPath(root, storageUnit, path)));
          }
          for (KeyRange keyRange : keyRanges) {
            fileSystemManager.trimFilesContent(
                fileList, tagFilter, keyRange.getActualBeginKey(), keyRange.getActualEndKey());
          }
        }
      } catch (IOException e) {
        return new TaskExecuteResult(
            new FilesystemException("encounter error when deleting data: ", e));
      }
    }
    return new TaskExecuteResult(null, null);
  }

  @Override
  public List<Column> getColumnsOfStorageUnit(String storageUnit) throws PhysicalException {
    List<Column> columns = new ArrayList<>();
    if (root != null) {
      File directory = new File(FilePathUtils.toIginxPath(root, storageUnit, null));
      for (File file : fileSystemManager.getAllFiles(directory, false)) {
        FileMeta meta = fileSystemManager.getFileMeta(file);
        if (meta == null) {
          throw new PhysicalException(
              String.format(
                  "encounter error when getting columns of storage unit because file meta %s is null",
                  file.getAbsolutePath()));
        }
        columns.add(
            new Column(
                FilePathUtils.convertAbsolutePathToPath(root, file.getAbsolutePath(), storageUnit),
                meta.getDataType(),
                meta.getTags(),
                false));
      }
    }
    if (hasData && dummyRoot != null) {
      for (File file : fileSystemManager.getAllFiles(new File(realDummyRoot), true)) {
        columns.add(
            new Column(
                FilePathUtils.convertAbsolutePathToPath(
                    dummyRoot, file.getAbsolutePath(), storageUnit),
                DataType.BINARY,
                null,
                true));
      }
    }
    return columns;
  }

  @Override
  public Pair<ColumnsInterval, KeyInterval> getBoundaryOfStorage(String dataPrefix)
      throws PhysicalException {
    KeyInterval keyInterval = KeyInterval.getDefaultKeyInterval();
    ColumnsInterval columnsInterval;

    File directory = new File(FilePathUtils.toNormalFilePath(realDummyRoot, dataPrefix));
    if (dataPrefix != null && !dataPrefix.isEmpty()) {
      columnsInterval = new ColumnsInterval(dataPrefix);
    } else {
      Pair<String, String> filePair = fileSystemManager.getBoundaryOfFiles(directory);
      if (filePair == null) {
        columnsInterval = new ColumnsInterval(prefix);
      } else {
        columnsInterval =
            new ColumnsInterval(
                FilePathUtils.convertAbsolutePathToPath(dummyRoot, filePair.k, null),
                FilePathUtils.convertAbsolutePathToPath(dummyRoot, filePair.v, null));
      }
    }

    return new Pair<>(columnsInterval, keyInterval);
  }

  @Override
  public void close() {
    fileSystemManager.close();
  }
}
