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

import static cn.edu.tsinghua.iginx.engine.logical.utils.PathUtils.MAX_CHAR;
import static cn.edu.tsinghua.iginx.filesystem.shared.Constant.*;

import cn.edu.tsinghua.iginx.auth.entity.FileAccessType;
import cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.file.DefaultFileOperator;
import cn.edu.tsinghua.iginx.filesystem.file.IFileOperator;
import cn.edu.tsinghua.iginx.filesystem.file.entity.FileMeta;
import cn.edu.tsinghua.iginx.filesystem.query.entity.FileSystemResultTable;
import cn.edu.tsinghua.iginx.filesystem.query.entity.Record;
import cn.edu.tsinghua.iginx.filesystem.tools.FilePathUtils;
import cn.edu.tsinghua.iginx.filesystem.tools.MemoryPool;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * 缓存，索引以及优化策略都在这里执行
 */
public class FileSystemManager {

  private final Logger LOGGER = LoggerFactory.getLogger(FileSystemManager.class);

  private final IFileOperator fileOperator;

  private final MemoryPool memoryPool;

  private final Map<String, FileMeta> fileMetaMap = new HashMap<>();

  private final int MULTI_THREAD_THRESHOLD = 1;

  public FileSystemManager(Map<String, String> params) {
    memoryPool =
        new MemoryPool(
            Integer.parseInt(params.getOrDefault(INIT_INFO_MEMORY_POOL_SIZE, "100")),
            Integer.parseInt(params.getOrDefault(INIT_INFO_CHUNK_SIZE, "1048576")));
    fileOperator = new DefaultFileOperator();
  }

  /** ***************** 查询相关 ******************** */
  public List<FileSystemResultTable> readFile(
      File file, TagFilter tagFilter, List<KeyRange> keyRanges, boolean isDummy)
      throws IOException {
    List<FileSystemResultTable> res = new ArrayList<>();
    // 首先通过tagFilter和file，找到所有有关的文件列表
    List<File> files = getFilesWithTagFilter(file, tagFilter, isDummy, false);
    for (File f : files) {
      List<Record> records = new ArrayList<>();
      // 根据keyRange过滤
      for (KeyRange keyRange : keyRanges) {
        long startKey = keyRange.getActualBeginKey();
        long endKey = keyRange.getActualEndKey();
        // 能直接添加的依据是，keyRange是递增的
        records.addAll(readSingleFile(f, startKey, endKey, isDummy)); // do readSingleFile here
      }
      if (!isDummy) {
        FileMeta fileMeta = getFileMeta(f);
        res.add(new FileSystemResultTable(f, records, fileMeta.getDataType(), fileMeta.getTags()));
      } else {
        res.add(new FileSystemResultTable(f, records, DataType.BINARY, new HashMap<>()));
      }
    }
    return res;
  }

  private List<File> getFilesWithTagFilter(
      File file, TagFilter tagFilter, boolean isDummy, boolean isWrite) throws IOException {
    List<File> associatedFiles = getAssociatedFiles(file, isDummy, isWrite);
    if (isDummy) {
      return associatedFiles;
    }
    List<File> res = new ArrayList<>();
    for (File f : associatedFiles) {
      FileMeta fileMeta = getFileMeta(f);
      if (tagFilter == null || TagKVUtils.match(fileMeta.getTags(), tagFilter)) {
        res.add(f);
      }
    }
    return res;
  }

  // 执行读文件，返回文件内容
  private List<Record> readSingleFile(File file, long startKey, long endKey, boolean isDummy)
      throws IOException {
    if (!isDummy) {
      FileMeta fileMeta = getFileMeta(file);
      return fileOperator.readIginxFile(file, startKey, endKey, fileMeta.getDataType());
    }

    // 处理dummy查询
    if (file == null || !file.exists() || !file.isFile()) {
      throw new IllegalArgumentException(
          String.format("invalid file %s", file == null ? "null" : file.getAbsolutePath()));
    }
    if (startKey < 0 || endKey < startKey) {
      throw new IllegalArgumentException(
          String.format("invalid byte range startKey: %d endKey: %d", startKey, endKey));
    }
    if (endKey > file.length()) {
      endKey = file.length() - 1;
    }
    ExecutorService executorService = null;
    List<Future<Void>> futures = new ArrayList<>();
    List<byte[]> res = new ArrayList<>();
    long size = endKey - startKey;
    int chunkSize = memoryPool.chunkSize;
    int round = (int) (size % chunkSize == 0 ? size / chunkSize : size / chunkSize + 1);
    for (int i = 0; i < round; i++) {
      res.add(new byte[0]);
    }
    AtomicLong readPos = new AtomicLong(startKey);
    AtomicInteger index = new AtomicInteger();
    // TODO The configuration here varies in different situations.
    boolean needMultithread = size / (chunkSize) > MULTI_THREAD_THRESHOLD;
    if (needMultithread) {
      executorService = Executors.newCachedThreadPool();
    }
    // Move the file pointer to the starting position
    try {
      while (readPos.get() < endKey) {
        long finalReadPos = readPos.get();
        int finalIndex = index.get();
        byte[] buffer = memoryPool.allocate();
        if (needMultithread) {
          futures.add(
              executorService.submit(
                  () -> {
                    byte[] bytes = fileOperator.readNormalFile(file, finalReadPos, buffer);
                    res.set(finalIndex, bytes);
                    return null;
                  }));
        } else {
          byte[] bytes = fileOperator.readNormalFile(file, finalReadPos, buffer);
          res.set(finalIndex, bytes);
        }
        index.getAndIncrement();
        readPos.addAndGet(chunkSize);
      }
      if (executorService != null) {
        executorService.shutdown();
        if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
          executorService.shutdownNow();
        }
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(
          String.format("interrupted while reading file %s", file.getAbsolutePath()), e);
    } finally {
      if (executorService != null) {
        executorService.shutdown();
      }
    }

    // 判断是否在读取文件时出错
    for (Future<Void> future : futures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(
            String.format("exception thrown while reading file %s", file.getAbsolutePath()), e);
      }
    }

    // 组织数据
    List<Record> records = new ArrayList<>();
    long key = startKey;
    for (byte[] record : res) {
      records.add(new Record(key, record));
      key += record.length;
    }

    return records;
  }

  /** ****************** 写入相关 ******************** */
  public synchronized void writeFiles(
      List<File> files, List<List<Record>> recordsList, List<Map<String, String>> tagsList)
      throws IOException {
    files =
        files.stream()
            .map(f -> FilePathUtils.normalize(f, FileAccessType.WRITE))
            .collect(Collectors.toList());

    for (int i = 0; i < files.size(); i++) {
      writeFile(files.get(i), recordsList.get(i), tagsList.get(i));
    }
  }

  private synchronized void writeFile(File file, List<Record> records, Map<String, String> tags)
      throws IOException {
    File f;
    // 判断是否已经创建了对应的文件
    File tmpFile = getFileWithTags(file, tags);
    // 如果是首次写入该序列
    if (tmpFile == null) {
      f = getFileIDAndCreate(file, records, tags);
    } else {
      f = tmpFile;
    }
    fileOperator.writeIginxFile(f, records);
  }

  /**
   * 根据提供的 tags 集合查找同名 / 近名的 .iginx 文件,其元数据 tags 与提供的集合相等。 若找到,返回该文件,否则返回 null。
   *
   * @param file 用于查找相同名或近名的父级目录
   * @param tags 用于匹配的 tags 集合
   * @return 元数据与 tags 相等的 .iginx 文件,否则返回 null
   */
  private File getFileWithTags(File file, Map<String, String> tags) throws IOException {
    for (File f : getAssociatedFiles(file, false, true)) {
      FileMeta fileMeta = getFileMeta(f);
      if ((tags == null || tags.isEmpty()) && fileMeta.getTags().isEmpty()) {
        return f;
      }
      if (Objects.equals(tags, fileMeta.getTags())) {
        return f;
      }
    }
    return null;
  }

  private synchronized File getFileIDAndCreate(
      File file, List<Record> records, Map<String, String> tags) throws IOException {
    // 判断该文件的后缀id
    File f = determineFileID(file);
    // 创建该文件
    FileMeta fileMeta = new FileMeta(records.get(0).getDataType(), tags);
    fileMetaMap.put(f.getAbsolutePath(), fileMeta);
    return fileOperator.create(f, fileMeta);
  }

  private File determineFileID(File file) throws IOException {
    int id = getFileID(file);
    if (id == -1) {
      id = 0;
    } else {
      id += 1;
    }
    return new File(file.getAbsolutePath() + id);
  }

  // 获取文件id，例如 a.iginx5，则其id就是5
  private int getFileID(File file) throws IOException {
    List<File> files = getAssociatedFiles(file, false, true);
    if (files.isEmpty()) {
      return -1;
    }
    List<Integer> nums = new ArrayList<>();
    nums.add(0);
    for (File f : files) {
      String name = f.getName();
      String numStr = name.substring(name.lastIndexOf(FILE_EXTENSION) + FILE_EXTENSION.length());
      if (numStr.isEmpty()) {
        continue;
      }
      nums.add(Integer.parseInt(numStr));
    }
    return Collections.max(nums);
  }

  /** ****************** 删除相关 ******************** */
  public void deleteFile(File file) throws IOException {
    deleteFiles(Collections.singletonList(file), null);
  }

  /**
   * 删除文件或目录
   *
   * @param files 要删除的文件或目录列表
   * @return 如果删除操作失败则抛出异常
   */
  public void deleteFiles(List<File> files, TagFilter filter) throws IOException {
    for (File file : files) {
      try {
        for (File f : getFilesWithTagFilter(file, filter, false, true)) {
          fileOperator.delete(f);
          fileMetaMap.remove(f.getAbsolutePath());
        }
      } catch (IOException e) {
        throw new IOException(String.format("delete file %s failed", file.getAbsoluteFile()), e);
      }
    }
  }

  public void trimFilesContent(List<File> files, TagFilter tagFilter, long startKey, long endKey)
      throws IOException {
    for (File file : files) {
      List<File> fileList = getFilesWithTagFilter(file, tagFilter, false, true);
      if (fileList.isEmpty()) {
        LOGGER.warn("cant trim the file that not exist!");
        continue;
      }
      for (File f : fileList) {
        fileOperator.trimFile(f, startKey, endKey);
      }
    }
  }

  // 返回和file文件相关的所有文件
  private List<File> getAssociatedFiles(File file, boolean isDummy, boolean isWrite)
      throws IOException {
    List<File> associatedFiles = new ArrayList<>();
    try {
      String filePath = file.getAbsolutePath();
      if (!filePath.contains(WILDCARD) && isDummy) {
        File checkedFile = FilePathUtils.normalize(file);
        if (checkedFile.isFile() && checkedFile.exists()) {
          associatedFiles.add(checkedFile);
        }
      } else { // filePath.contains(WILDCARD) || !isDummy
        File root;
        String regex;
        if (filePath.contains(WILDCARD)) {
          root = new File(filePath.substring(0, filePath.indexOf(WILDCARD)));
        } else { // !isDummy
          root = file.getParentFile();
        }
        root = FilePathUtils.normalize(root);
        if (isDummy) {
          regex = filePath.replaceAll("[$^{}\\\\]", "\\\\$0").replaceAll("[*]", ".*");
        } else {
          regex = filePath.replaceAll("[$^{}\\\\]", "\\\\$0").replaceAll("[*]", ".*") + ".*";
        }
        Files.walkFileTree(
            root.toPath(),
            new FileVisitor<Path>() {
              @Override
              public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                return FileVisitResult.CONTINUE;
              }

              @Override
              public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (!file.toFile().isHidden() && Pattern.matches(regex, file.toString())) {
                  associatedFiles.add(file.toFile());
                }
                return FileVisitResult.CONTINUE;
              }

              @Override
              public FileVisitResult visitFileFailed(Path file, IOException exc) {
                return FileVisitResult.CONTINUE;
              }

              @Override
              public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                return FileVisitResult.CONTINUE;
              }
            });
      }
    } catch (IOException e) {
      throw new IOException(
          String.format("get associated files of %s failure: %s", file.getAbsolutePath(), e));
    }
    return associatedFiles.stream()
        .map(f -> FilePathUtils.normalize(f, isWrite ? FileAccessType.WRITE : FileAccessType.READ))
        .collect(Collectors.toList());
  }

  public List<File> getAllFiles(File dir, boolean containsEmptyDir) {
    dir = FilePathUtils.normalize(dir, FileAccessType.READ);

    List<File> res = new ArrayList<>();
    try {
      Files.walkFileTree(
          dir.toPath(),
          new FileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                throws IOException {
              if (containsEmptyDir && isDirEmpty(dir)) {
                res.add(dir.toFile());
              }
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
              res.add(file.toFile());
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (IOException e) {
      LOGGER.error("get all files of {} failure: ", dir.getAbsolutePath(), e);
    }
    return res;
  }

  // 返回字典序最大和最小的文件路径，可能是目录
  public Pair<String, String> getBoundaryOfFiles(File dir) {
    dir = FilePathUtils.normalize(dir, FileAccessType.READ);

    File[] files = dir.listFiles();
    if (files == null || files.length == 0) {
      LOGGER.error("{} is empty", dir.getAbsolutePath());
      return null;
    }
    Arrays.sort(files);
    File minFile = files[0];
    File maxFile =
        new File(
            Paths.get(files[files.length - 1].getAbsolutePath(), String.valueOf(MAX_CHAR))
                .toString());
    return new Pair<>(minFile.getAbsolutePath(), maxFile.getAbsolutePath());
  }

  public FileMeta getFileMeta(File file) {
    file = FilePathUtils.normalize(file, FileAccessType.READ);

    try {
      FileMeta fileMeta;
      String filePath = file.getAbsolutePath();
      if (fileMetaMap.containsKey(filePath)) {
        fileMeta = fileMetaMap.get(filePath);
      } else {
        fileMeta = fileOperator.getFileMeta(file);
        fileMetaMap.put(filePath, fileMeta);
      }
      return fileMeta;
    } catch (IOException e) {
      LOGGER.error("get file meta of {} failure", file.getAbsolutePath(), e);
    }
    return null;
  }

  private boolean isDirEmpty(Path dir) throws IOException {
    try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(dir)) {
      return !dirStream.iterator().hasNext();
    }
  }

  /** ***************** 资源控制 ******************** */
  public MemoryPool getMemoryPool() {
    return memoryPool;
  }

  public void close() {
    memoryPool.close();
  }
}
