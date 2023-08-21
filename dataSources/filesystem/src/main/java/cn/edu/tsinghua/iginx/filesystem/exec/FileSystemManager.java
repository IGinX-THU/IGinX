package cn.edu.tsinghua.iginx.filesystem.exec;

import static cn.edu.tsinghua.iginx.engine.logical.utils.PathUtils.MAX_CHAR;
import static cn.edu.tsinghua.iginx.filesystem.constant.Constant.*;

import cn.edu.tsinghua.iginx.engine.physical.storage.utils.TagKVUtils;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.file.DefaultFileOperator;
import cn.edu.tsinghua.iginx.filesystem.file.IFileOperator;
import cn.edu.tsinghua.iginx.filesystem.file.entity.FileMeta;
import cn.edu.tsinghua.iginx.filesystem.query.entity.FileSystemResultTable;
import cn.edu.tsinghua.iginx.filesystem.query.entity.Record;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * 缓存，索引以及优化策略都在这里执行
 */
public class FileSystemManager {

  private static final Logger logger = LoggerFactory.getLogger(FileSystemManager.class);

  private static final IFileOperator fileOperator = DefaultFileOperator.getInstance();

  private static final Map<String, FileMeta> fileMetaMap = new HashMap<>();

  // set the fileSystem type with constructor
  public FileSystemManager(/*FileSystemType type*/ ) {}

  /** ******************** 查询相关 ******************** */
  public static List<FileSystemResultTable> readFile(
      File file, TagFilter tagFilter, List<KeyRange> keyRanges, boolean isDummy)
      throws IOException {
    List<FileSystemResultTable> res = new ArrayList<>();
    // 首先通过tagFilter和file，找到所有有关的文件列表
    List<File> files = getFilesWithTagFilter(file, tagFilter, isDummy);
    for (File f : files) {
      List<Record> records = new ArrayList<>();
      // 根据keyRange过滤
      for (KeyRange keyRange : keyRanges) {
        long startKey = keyRange.getActualBeginKey();
        long endKey = keyRange.getActualEndKey();
        // 能直接添加的依据是，keyRange是递增的
        records.addAll(readSingleFile(f, startKey, endKey, isDummy)); // do readFile here
      }
      if (!isDummy) {
        FileMeta fileMeta = fileOperator.getFileMeta(f);
        res.add(new FileSystemResultTable(f, records, fileMeta.getDataType(), fileMeta.getTags()));
      } else {
        res.add(new FileSystemResultTable(f, records, DataType.BINARY, null));
      }
    }
    return res;
  }

  private static List<File> getFilesWithTagFilter(File file, TagFilter tagFilter, boolean isDummy)
      throws IOException {
    List<File> files = getAssociatedFiles(file);
    List<File> res = new ArrayList<>();
    for (File f : files) {
      if (isDummy) {
        res.add(f);
      } else {
        FileMeta fileMeta = fileOperator.getFileMeta(f);
        if (tagFilter == null || TagKVUtils.match(fileMeta.getTags(), tagFilter)) {
          res.add(f);
        }
      }
    }
    return res;
  }

  // 执行读文件，返回文件内容
  private static List<Record> readSingleFile(File file, long startKey, long endKey, boolean isDummy)
      throws IOException {
    if (isDummy) {
      return fileOperator.readNormalFile(file, startKey, endKey, CHARSET);
    } else {
      return fileOperator.readIginxFile(file, startKey, endKey, CHARSET);
    }
  }

  /** ******************** 写入相关 ******************** */
  public static synchronized Exception writeFiles(
      List<File> files, List<List<Record>> recordsList, List<Map<String, String>> tagsList)
      throws IOException {
    for (int i = 0; i < files.size(); i++) {
      Exception e = writeFile(files.get(i), recordsList.get(i), tagsList.get(i));
      if (e != null) {
        return e;
      }
    }
    return null;
  }

  private static synchronized Exception writeFile(
      File file, List<Record> records, Map<String, String> tags) throws IOException {
    File f;
    // 判断是否已经创建了对应的文件
    File tmpFile = getFileWithTags(file, tags);
    // 如果是首次写入该序列
    if (tmpFile == null) {
      f = getFileIDAndCreate(file, records, tags);
    } else {
      f = tmpFile;
    }
    return fileOperator.writeIginxFile(f, records);
  }

  /**
   * 根据提供的 tags 集合查找同名 / 近名的 .iginx 文件,其元数据 tags 与提供的集合相等。 若找到,返回该文件,否则返回 null。
   *
   * @param file 用于查找相同名或近名的父级目录
   * @param tags 用于匹配的 tags 集合
   * @return 元数据与 tags 相等的 .iginx 文件,否则返回 null
   * @throws IOException 任何查找或读写操作导致的 IOException 将被传播
   */
  private static File getFileWithTags(File file, Map<String, String> tags) throws IOException {
    List<File> files = getAssociatedFiles(file);
    for (File f : files) {
      FileMeta fileMeta = fileOperator.getFileMeta(f);
      if ((tags == null || tags.isEmpty()) && fileMeta.getTags().isEmpty()
          || Objects.equals(tags, fileMeta.getTags())) {
        return f;
      }
    }
    return null;
  }

  private static synchronized File getFileIDAndCreate(
      File file, List<Record> records, Map<String, String> tags) throws IOException {
    // 判断该文件的后缀id
    File f = determineFileId(file, tags);
    // 创建该文件
    return fileOperator.create(f, new FileMeta(records.get(0).getDataType(), tags));
  }

  private static File determineFileId(File file, Map<String, String> tag) throws IOException {
    int id = getFileID(file, tag);
    if (id == -1) {
      id = 0;
    } else {
      id += 1;
    }
    String path = file.getAbsolutePath() + id;
    return new File(path);
  }

  // 获取文件id，例如 a.iginx5，则其id就是5
  private static int getFileID(File file, Map<String, String> tag) throws IOException {
    List<File> files = getAssociatedFiles(file);
    if (files.isEmpty()) {
      return -1;
    }

    List<Integer> nums = new ArrayList<>();
    nums.add(0);
    for (File f : files) {
      String name = f.getName();
      int idx = name.lastIndexOf(FILE_EXTENSION);
      String numStr = name.substring(idx + FILE_EXTENSION.length());
      if (numStr.isEmpty()) continue;
      nums.add(Integer.parseInt(numStr));
    }

    return Collections.max(nums);
  }

  /** ******************** 删除相关 ******************** */
  public static Exception deleteFile(File file) {
    return deleteFiles(Collections.singletonList(file), null);
  }

  /**
   * 删除文件或目录
   *
   * @param files 要删除的文件或目录列表
   * @return 如果删除操作失败则抛出异常
   */
  public static Exception deleteFiles(List<File> files, TagFilter filter) {
    List<File> fileList = new ArrayList<>();
    try {
      for (File file : files) {
        fileList.addAll(getFilesWithTagFilter(file, filter, false));
      }
    } catch (IOException e) {
      logger.error(
          "delete files {} failure: {}",
          files.stream().map(File::getAbsolutePath).collect(Collectors.joining(" ")),
          e.getMessage());
      return e;
    }
    for (File file : fileList) {
      if (!fileOperator.delete(file)) {
        return new IOException("Failed to delete file: " + file.getAbsolutePath());
      }
    }
    return null;
  }

  public static Exception trimFilesContent(
      List<File> files, TagFilter tagFilter, long startKey, long endKey) throws IOException {
    for (File file : files) {
      List<File> fileList = getFilesWithTagFilter(file, tagFilter, false);
      if (fileList.isEmpty()) {
        logger.warn("cant trim the file that not exist!");
        continue;
      }
      for (File f : fileList) {
        Exception e = fileOperator.trimFile(f, startKey, endKey);
        if (e != null) {
          return e;
        }
      }
    }
    return null;
  }

  // 返回和file文件相关的所有文件
  private static List<File> getAssociatedFiles(File file) {
    List<File> fileList;
    Stack<File> S = new Stack<>();
    Set<File> res = new HashSet<>();
    File root;
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
            if (f.isDirectory()) {
              S.push(f);
            }
          }
        }
      }
      if (flag) {
        if (tmp.isFile()) {
          res.add(tmp);
        }
      }
      flag = true;
    }
    fileList = new ArrayList<>(res);
    return fileList;
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
      if (fileList == null) {
        continue;
      }
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

  // 返回字典序最大和最小的文件，可能是目录
  public static Pair<File, File> getBoundaryOfFiles(File dir) {
    File[] files = dir.listFiles();
    if (files == null || files.length == 0) {
      logger.error("{} is empty", dir.getAbsolutePath());
      return null;
    }
    Arrays.sort(files);
    File minFile = files[0];
    File maxFile =
        new File(
            Paths.get(files[files.length - 1].getAbsolutePath(), String.valueOf(MAX_CHAR))
                .toString());
    return new Pair<>(minFile, maxFile);
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
