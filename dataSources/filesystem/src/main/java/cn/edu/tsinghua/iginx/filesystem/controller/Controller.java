package cn.edu.tsinghua.iginx.filesystem.controller;

import static cn.edu.tsinghua.iginx.engine.logical.utils.ExprUtils.getKeyRangesFromFilter;
import static cn.edu.tsinghua.iginx.filesystem.constant.Constant.FILE_EXTENSION;
import static cn.edu.tsinghua.iginx.filesystem.constant.Constant.WILDCARD;

import cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.file.DefaultFileOperator;
import cn.edu.tsinghua.iginx.filesystem.file.IFileOperator;
import cn.edu.tsinghua.iginx.filesystem.file.entity.FileMeta;
import cn.edu.tsinghua.iginx.filesystem.file.type.FileType;
import cn.edu.tsinghua.iginx.filesystem.query.entity.FileSystemResultTable;
import cn.edu.tsinghua.iginx.filesystem.query.entity.Record;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * 缓存，索引以及优化策略都在这里执行
 */
public class Controller {
  private static final Logger logger = LoggerFactory.getLogger(Controller.class);
  private static IFileOperator fileOperator = new DefaultFileOperator();
  private static Charset charset = StandardCharsets.UTF_8;

  // set the fileSystem type with constructor
  public Controller(/*FileSystemType type*/ ) {
    fileOperator = new DefaultFileOperator();
  }

  public static List<FileSystemResultTable> readFile(File file, Filter filter) throws IOException {
    return readFile(file, null, filter);
  }

  // 获取经过keyFilter后的val值
  public static List<FileSystemResultTable> getValWithFilter(
      List<File> files, List<KeyRange> keyRanges, Filter filter) throws IOException {
    List<FileSystemResultTable> res = new ArrayList<>();

    for (File f : files) {
      List<Record> val = new ArrayList<>();
      for (KeyRange keyRange : keyRanges) {
        long startTime = keyRange.getActualBeginKey(), endTime = keyRange.getActualEndKey();
        // 能直接添加的依据是，keyrange是递增的
        val.addAll(doReadFile(f, startTime, endTime)); // do read file here
      }
      if (FileType.getFileType(f) == FileType.IGINX_FILE) {
        FileMeta fileMeta = fileOperator.getFileMeta(f);
        res.add(new FileSystemResultTable(f, val, fileMeta.getDataType(), fileMeta.getTags()));
      } else {
        res.add(new FileSystemResultTable(f, val, DataType.BINARY, null));
      }
    }
    return res;
  }

  public static List<FileSystemResultTable> readFile(File file, TagFilter tagFilter, Filter filter)
      throws IOException {
    // 首先通过tag和file，找到所有有关的文件列表
    List<File> files = getFilesWithTagFilter(file, tagFilter);
    if (files == null) {
      return new ArrayList<>();
    }

    List<KeyRange> keyRanges = getKeyRangesFromFilter(filter);
    if (keyRanges.size() == 0) {
      keyRanges.add(new KeyRange(0, Long.MAX_VALUE));
    }
    return getValWithFilter(files, keyRanges, filter);
  }

  // TODO:返回空值
  public static List<Record> readFile(File file, long begin, long end) throws IOException {
    return doReadFile(file, begin, end);
  }

  // 执行读文件，返回文件内容
  private static List<Record> doReadFile(File file, long begin, long end) throws IOException {
    List<Record> res = new ArrayList<>();
    switch (FileType.getFileType(file)) {
      case DIR:
        logger.error("{} is a dir!", file.getAbsolutePath());
        break;
      case IGINX_FILE:
        res = fileOperator.readIGinXFileByKey(file, begin, end, charset);
        break;
      case NORMAL_FILE:
      default:
        res = fileOperator.readNormalFile(file, begin, end, charset);
        break;
    }
    return res;
  }

  public static boolean mkDir(File file) {
    return fileOperator.mkdir(file);
  }

  public static Exception writeFile(File file, List<Record> value) throws IOException {
    return doWriteFile(file, value);
  }

  public static synchronized File getIDAndCreate(
      File file, List<Record> value, Map<String, String> tag) throws IOException {
    File f = null;
    // 判断该文件的后缀id
    f = determineFileId(file, tag);
    // 创建该文件
    f = createIginxFile(f, value.get(0).getDataType(), tag);
    return f;
  }

  public static Exception writeFile(File file, List<Record> value, Map<String, String> tag)
      throws IOException {
    File tmpFile, f;
    // 判断是否已经创建了对应的文件
    tmpFile = getFileWithTag(file, tag);
    // 如果是首次写入该序列
    if (tmpFile == null) {
      f = getIDAndCreate(file, value, tag);
    } else {
      f = tmpFile;
    }

    return doWriteFile(f, value);
  }

  public static Exception writeFiles(List<File> files, List<List<Record>> values)
      throws IOException {
    return writeFiles(files, values, null);
  }

  // write multi file
  public static Exception writeFiles(
      List<File> files, List<List<Record>> values, List<Map<String, String>> tagList)
      throws IOException {
    for (int i = 0; i < files.size(); i++) {
      Exception e = writeFile(files.get(i), values.get(i), tagList.get(i));
      if (e != null) {
        return e;
      }
    }
    return null;
  }

  public static Exception deleteFile(File file) throws IOException {
    return deleteFiles(Collections.singletonList(file), null);
  }

  /**
   * 删除文件或目录
   *
   * @param files 要删除的文件或目录列表
   * @throws Exception 如果删除操作失败则抛出异常
   */
  public static Exception deleteFiles(List<File> files, TagFilter filter) throws IOException {
    List<File> fileList = new ArrayList<>();
    for (File file : files) {
      List<File> tmp = getFilesWithTagFilter(file, filter);
      if (tmp == null) continue;
      fileList.addAll(tmp);
    }
    for (File file : fileList) {
      if (!fileOperator.delete(file)) {
        return new IOException("Failed to delete file: " + file.getAbsolutePath());
      }
    }
    return null;
  }

  public static Exception trimFileContent(File file, long begin, long end) throws IOException {
    return trimFilesContent(Collections.singletonList(file), null, begin, end);
  }

  public static Exception trimFileContent(File file, TagFilter tagFilter, long begin, long end)
      throws IOException {
    return trimFilesContent(Collections.singletonList(file), tagFilter, begin, end);
  }

  public static Exception trimFilesContent(
      List<File> files, TagFilter tagFilter, long begin, long end) throws IOException {
    for (File file : files) {
      List<File> fileList = getFilesWithTagFilter(file, tagFilter);
      if (fileList == null) {
        logger.warn("cant trim the file that not exist!");
        continue;
      }
      for (File f : fileList) {
        fileOperator.trimFile(f, begin, end);
      }
    }
    return null;
  }

  private static File createIginxFile(File file, DataType dataType, Map<String, String> tag)
      throws IOException {
    return fileOperator.create(file, new FileMeta(dataType, tag));
  }

  private static boolean ifFileExists(File file, TagFilter tagFilter) throws IOException {
    if (getFilesWithTagFilter(file, tagFilter) != null) {
      return true;
    }
    return false;
  }

  private static boolean ifFileExists(File file) {
    return fileOperator.ifFileExists(file);
  }

  // 获取文件id，例如 a.iginx5，则其id就是5
  private static int getFileID(File file, Map<String, String> tag) throws IOException {
    List<File> files = getAssociatedFiles(file);

    List<Integer> nums = new ArrayList<>();
    nums.add(0);
    if (files == null) return -1;
    for (File f : files) {
      String name = f.getName();
      if (fileOperator.isDirectory(f)) continue;
      int idx = name.lastIndexOf(FILE_EXTENSION);
      String numStr = name.substring(idx + FILE_EXTENSION.length());
      if (numStr.isEmpty()) continue;
      nums.add(Integer.parseInt(numStr));
    }

    return Collections.max(nums);
  }

  private static File determineFileId(File file, Map<String, String> tag) throws IOException {
    int id = getFileID(file, tag);
    if (id == -1) id = 0;
    else id += 1;
    String path = file.getAbsolutePath() + id;
    return new File(path);
  }

  private static Exception doWriteFile(File file, List<Record> value) throws IOException {
    Exception res = null;

    switch (FileType.getFileType(file)) {
      case DIR:
        if (!mkDir(file)) {
          logger.error("create dir fail!");
          throw new IOException("create dir fail!");
        }
        break;
      case IGINX_FILE:
        res = fileOperator.writeIGinXFile(file, value);
        break;
      case NORMAL_FILE:
        break;
      default:
        res = fileOperator.writeIGinXFile(file, value);
    }
    return res;
  }

  // 返回和file文件相关的所有文件（包括目录）
  public static List<File> getAssociatedFiles(File file) {
    List<File> fileList, filess = null;
    Stack<File> S = new Stack<>();
    Set<File> res = new HashSet<>();
    File root = null;
    String prefix = null;
    // select * from *
    if (file.getParentFile().getName().equals(WILDCARD) && file.getName().contains(WILDCARD)) {
      root = file.getParentFile().getParentFile(); // storage unit file
    } else if (file.getParentFile().getName().equals(WILDCARD)
        && !file.getName().contains(WILDCARD)) {
      File tmp = file.getParentFile();
      while (tmp.getName().equals(WILDCARD)) {
        tmp = tmp.getParentFile();
      }
      root = tmp;
      prefix = file.getName();
    } else if (file.getName().contains(WILDCARD)) {
      root = file.getParentFile();
    } else if (file.isDirectory()) {
      root = file;
    } else {
      root = file.getParentFile();
      prefix = file.getName();
    }
    boolean flag = false;
    S.push(root);
    while (!S.empty()) {
      File tmp = S.pop();
      if (tmp.isDirectory()) {
        List<File> files = fileOperator.listFiles(tmp, prefix),
            dirlist = fileOperator.listFiles(tmp);
        if (files != null) {
          for (File f : files) S.push(f);
        }
        if (dirlist != null) {
          for (File f : dirlist) {
            if (f.isDirectory()) S.push(f);
          }
        }
      }
      if (flag) res.add(tmp);
      flag = true;
    }
    fileList = new ArrayList<>(res);
    return fileList.size() == 0 ? null : fileList;
  }

  /**
   * 根据提供的 tags 集合查找同名 / 近名的 .iginx 文件,其元数据 tags 与提供的集合相等。 若找到,返回该文件,否则返回 null。
   *
   * @param tags 用于匹配的 tags 集合
   * @param file 用于查找相同名或近名的父级目录
   * @return 元数据与 tags 相等的 .iginx 文件,否则返回 null
   * @throws IOException 任何查找或读写操作导致的 IOException 将被传播
   */
  private static File getFileWithTag(File file, Map<String, String> tags) throws IOException {
    List<File> res = getAssociatedFiles(file);
    if (res == null) return null;
    for (File fi : res) {
      if (fi.isDirectory()) continue;
      FileMeta fileMeta = fileOperator.getFileMeta(fi);
      if ((tags == null || tags.size() == 0) && fileMeta.getTags() == null
          || Objects.equals(tags, fileMeta.getTags())) {
        return fi;
      }
    }
    return null;
  }

  private static List<File> getFilesWithTagFilter(File file, TagFilter tagFilter)
      throws IOException {
    List<File> files = getAssociatedFiles(file), res = new ArrayList<>();
    if (files == null) return files;
    for (File fi : files) {
      if (fi.isDirectory() || !fileOperator.ifFileExists(fi)) continue;
      FileMeta fileMeta = null;
      if (FileType.getFileType(fi) == FileType.IGINX_FILE) fileMeta = fileOperator.getFileMeta(fi);
      if (tagFilter == null || TagKVUtils.match(fileMeta.getTags(), tagFilter)) {
        res.add(fi);
      }
    }
    return res.size() == 0 ? null : res;
  }

  public static List<File> getAllFilesWithoutDir(File dir) {
    List<File> res = new ArrayList<>();
    Stack<File> stack = new Stack<>();
    stack.push(dir);
    while (!stack.isEmpty()) {
      File current = stack.pop();
      List<File> fileList = null;
      if (current.isDirectory()) {
        fileList = fileOperator.listFiles(current);
      }
      if (fileList == null) continue;
      for (File f : fileList) {
        if (f.isDirectory()) {
          stack.push(f);
        } else {
          res.add(f);
        }
      }
    }
    return res;
  }

  // 返回最大最小的文件，文件可能是目录
  public static Pair<File, File> getBoundaryFiles(File dir) {
    File minFile = getMinFile(dir);
    File maxFile = getMaxFile(dir);

    File lastFile = null;
    if (maxFile == null) {
      return null;
    }
    while (maxFile.isDirectory()) {
      File[] files = maxFile.listFiles();
      if (files != null && files.length != 0) maxFile = files[files.length - 1];
      if (lastFile != null && fileOperator.ifFilesEqual(lastFile, maxFile)) {
        break;
      }
      lastFile = maxFile;
    }

    return new Pair<>(minFile, maxFile);
  }

  static File getMinFile(File dir) {
    File[] files = dir.listFiles();
    if (files == null) {
      return null;
    }
    Arrays.sort(files);
    return files[0];
  }

  static File getMaxFile(File dir) {
    File[] files = dir.listFiles();
    if (files == null) {
      return null;
    }
    if (files.length == 0) {
      return dir;
    }
    Arrays.sort(files);
    return files[files.length - 1];
  }

  public static Long getMaxTime(File dir) {
    List<File> files = getAssociatedFiles(dir);
    long max = Long.MIN_VALUE;

    for (File f : files) {
      long size = getFileSize(f);
      max = Math.max(max, size);
    }

    return max;
  }

  private static long getFileSize(File file) {
    try {
      if (file.exists() && file.isFile()) {
        return fileOperator.length(file);
      }
    } catch (IOException e) {
      logger.error(e.getMessage());
    }

    return 0;
  }

  public static FileMeta getFileMeta(File file) {
    try {
      return fileOperator.getFileMeta(file);
    } catch (IOException e) {
      logger.error(e.getMessage());
    }
    return null;
  }
}
