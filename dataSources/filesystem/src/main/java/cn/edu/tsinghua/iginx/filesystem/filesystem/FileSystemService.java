package cn.edu.tsinghua.iginx.filesystem.filesystem;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.filesystem.file.IFileOperator;
import cn.edu.tsinghua.iginx.filesystem.file.entity.DefaultFileOperator;
import cn.edu.tsinghua.iginx.filesystem.file.property.FileMeta;
import cn.edu.tsinghua.iginx.filesystem.file.property.FilePath;
import cn.edu.tsinghua.iginx.filesystem.file.property.FileType;
import cn.edu.tsinghua.iginx.filesystem.query.FSResultTable;
import cn.edu.tsinghua.iginx.filesystem.thrift.FSFilter;
import cn.edu.tsinghua.iginx.filesystem.tools.ConfLoader;
import cn.edu.tsinghua.iginx.filesystem.tools.FilterTransformer;
import cn.edu.tsinghua.iginx.filesystem.tools.TagKVUtils;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.parsers.ExprParser;
import com.bpodgursky.jbool_expressions.rules.RuleSet;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.util.tuples.Triplet;

/*
 * 缓存，索引以及优化策略都在这里执行
 */
public class FileSystemService {
  private static final Logger logger = LoggerFactory.getLogger(FileSystemService.class);
  private static IFileOperator fileOperator = new DefaultFileOperator();
  private static Charset charset = StandardCharsets.UTF_8;
  private static String WILDCARD = FilePath.WILDCARD;

  // set the fileSystem type with constructor
  public FileSystemService(/*FileSystemType type*/ ) {
    fileOperator = new DefaultFileOperator();
    FilePath.setSeparator(System.getProperty("file.separator"));
  }

  public static List<FSResultTable> readFile(File file, FSFilter filter) throws IOException {
    return readFile(file, null, filter);
  }

  public static List<List<String>> expressionToParts(String expr) {
    List<List<String>> result = new ArrayList<>();
    int start = 0, left = 0, right = 0;
    for (int i = 0; i < expr.length(); i++) {
      char c = expr.charAt(i);
      if (c == '(') {
        left++;
        right = expr.indexOf(")", left);
      }

      if (c == '|') {
        result.add(StringUtils.splitAround(expr, left + 1, right, "&"));
        start = i;
      }
    }
    if (start < expr.length()) {
      if (expr.contains("(")) {
        left = expr.indexOf("(", start);
        right = expr.indexOf(")", left);
        result.add(StringUtils.splitAround(expr, left + 1, right, "&"));
      } else {
        List<String> part = new ArrayList<>(Collections.singleton(expr));
        result.add(part);
      }
    }
    return result;
  }

  private static Pair<Long, Long> parseKey(List<String> part, BiMap<String, String> vals) {
    Long minTime = Long.MAX_VALUE, maxTime = 0L;
    for (String p : part) {
      String val = vals.get(p);
      if (val.contains("key")) {
        String[] parts = val.split(" ");
        String op = parts[1];
        switch (Op.str2Op(op)) {
          case L:
          case LE:
            maxTime = Math.max(maxTime, Long.parseLong(parts[2]));
            break;
          case GE:
          case G:
            minTime = Math.min(minTime, Long.parseLong(parts[2]));
            break;
          default:
            break;
        }
      }
    }
    return new Pair<>(
        minTime == Long.MAX_VALUE ? 0 : minTime, maxTime == 0 ? Long.MAX_VALUE : maxTime);
  }

  private static List<Triplet<String, Op, Object>> parseVal(
      List<String> part, BiMap<String, String> vals) {
    List<Triplet<String, Op, Object>> res = new ArrayList<>();
    for (String p : part) {
      String val = vals.get(p);
      if (!val.contains("key")) {
        String[] parts = val.split(" ");
        res.add(new Triplet<>(parts[0], Op.str2Op(parts[1]), parts[2]));
      }
    }
    return res;
  }

  private static List<Pair<Pair<Long, Long>, List<Triplet<String, Op, Object>>>> parseFilter(
      FSFilter filter) {
    List<Pair<Pair<Long, Long>, List<Triplet<String, Op, Object>>>> res = new ArrayList<>();

    BiMap<String, String> vals = HashBiMap.create();
    String filterExp = FilterTransformer.toString(filter, vals);
    Expression<String> nonStandard = ExprParser.parse(filterExp);
    Expression<String> sopForm = RuleSet.toDNF(nonStandard);

    List<List<String>> parts = expressionToParts(sopForm.toString());
    for (List<String> l : parts) {
      Pair<Long, Long> keys = parseKey(l, vals);
      List<Triplet<String, Op, Object>> valFilter = parseVal(l, vals);
      res.add(new Pair<>(keys, valFilter));
    }
    return res;
  }

  public static int compareObjects(DataType dataType, Object a, Object b) {
    switch (dataType) {
      case FLOAT:
        return Float.compare((float) a, (float) b);
      case DOUBLE:
        return Double.compare((float) a, (float) b);
      case BINARY:
        return (new String((byte[]) a)).compareTo(new String((byte[]) b));
      case INTEGER:
        return Integer.compare((int) a, (int) b);
      case LONG:
        return Long.compare((long) a, (long) b);
      default:
        logger.error("cant compare the val!");
        throw new IllegalArgumentException("cant compare the val with different type");
    }
  }

  public static List<FSResultTable> getValWithFilter(
      List<File> files, List<Pair<Pair<Long, Long>, List<Triplet<String, Op, Object>>>> filters)
      throws IOException {
    List<FSResultTable> res = new ArrayList<>();
    for (File f : files) {
      for (Pair<Pair<Long, Long>, List<Triplet<String, Op, Object>>> ft : filters) {
        long startTime = ft.k.k, endTime = ft.k.v;
        List<Record> val = new ArrayList<>(), valf = new ArrayList<>();
        val = doReadFile(f, startTime, endTime); // do read file here

        if (FileType.getFileType(f) == FileType.IGINX_FILE) {
          List<Triplet<String, Op, Object>> valFilters = ft.v;
          if (valFilters.size() == 0) valf = val;
          for (Triplet<String, Op, Object> valFilter : valFilters) {
            if (f.getAbsolutePath()
                .contains(valFilter.getA().replaceAll(".", FilePath.getSEPARATOR()))) {
              for (Record record : val) {
                Object data = record.getRawData();
                Object dataf = valFilter.getC();
                boolean flag = false;
                int comRes = compareObjects(record.getDataType(), data, dataf);
                switch (valFilter.getB()) {
                  case L:
                    if (comRes < 0) flag = true;
                    break;
                  case LE:
                    if (comRes <= 0) flag = true;
                  case G:
                    if (comRes > 0) flag = true;
                  case GE:
                    if (comRes >= 0) flag = true;
                  case LIKE:
                    String s = new String((byte[]) data), format = new String((byte[]) dataf);
                    if (s.matches(format)) flag = true;
                  case NE:
                    if (comRes != 0) flag = true;
                }
                if (flag) valf.add(new Record(record.getKey(), record.getDataType(), data));
              }
              val = valf;
            }
          }

          FileMeta fileMeta = fileOperator.getFileMeta(f);
          res.add(new FSResultTable(f, valf, fileMeta.getDataType(), fileMeta.getTag()));
        } else {
          res.add(new FSResultTable(f, val, DataType.BINARY, null));
        }
      }
    }
    return res;
  }

  public static List<FSResultTable> readFile(File file, TagFilter tagFilter, FSFilter filter)
      throws IOException {
    List<File> files = getFilesWithTagFilter(file, tagFilter);
    if (files == null) {
      return new ArrayList<>();
    }

    List<Pair<Pair<Long, Long>, List<Triplet<String, Op, Object>>>> fts = parseFilter(filter);

    return getValWithFilter(files, fts);
  }

  // TODO:返回空值
  public static List<Record> readFile(File file, long begin, long end) throws IOException {
    return doReadFile(file, begin, end);
  }

  private static List<Record> doReadFile(File file, long begin, long end) throws IOException {
    List<Record> res = new ArrayList<>();
    switch (FileType.getFileType(file)) {
      case DIR:
        res = fileOperator.readDir(file);
        break;
      case IGINX_FILE:
        res = fileOperator.readIGinXFileByKey(file, begin, end, charset);
        break;
      case NORMAL_FILE:
        res = fileOperator.readNormalFile(file, begin, end, charset);
        break;
      default:
        res = fileOperator.readNormalFile(file, begin, end, charset);
    }
    return res;
  }

  public static boolean mkDir(File file) {
    return fileOperator.mkdir(file);
  }

  public static Exception writeFile(File file, List<Record> value) throws IOException {
    return doWriteFile(file, value);
  }

  public static Exception writeFile(File file, List<Record> value, Map<String, String> tag)
      throws IOException {
    File tmpFile;
    tmpFile = getFileWithTag(file, tag);
    if (tmpFile == null) {
      file = determineFileId(file, tag);
      file = createIginxFile(file, value.get(0).getDataType(), tag);
    } else {
      file = tmpFile;
    }

    return doWriteFile(file, value);
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
      File parent = file.getParentFile();
      while (parent != null && parent.isDirectory() && fileOperator.listFiles(parent) == null) {
        if (!fileOperator.delete(parent)) {
          return new IOException("Failed to delete file: " + file.getAbsolutePath());
        }
        parent = parent.getParentFile();
        if (fileOperator.ifFilesEqual(parent.getParentFile(), ConfLoader.getRootFile())) break;
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

  private static int getFileID(File file, Map<String, String> tag) throws IOException {
    List<File> files = getAssociatedFiles(file);

    List<Integer> nums = new ArrayList<>();
    nums.add(0);
    if (files == null) return -1;
    for (File f : files) {
      String name = f.getName();
      if (fileOperator.isDirectory(f)) continue;
      int idx = name.lastIndexOf(".iginx");
      String numStr = name.substring(idx + 6);
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

  private static byte[] makeValueToBytes(List<Record> value) throws IOException {
    List<byte[]> byteList = new ArrayList<>();
    switch (value.get(0).getDataType()) {
      case BINARY:
      default:
        for (Record val : value) {
          byteList.add((byte[]) val.getRawData());
        }
        break;
    }
    return mergeByteArrays(byteList);
  }

  public static byte[] mergeByteArrays(List<byte[]> arrays) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    for (byte[] array : arrays) {
      outputStream.write(array);
    }
    return outputStream.toByteArray();
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
      if ((tags == null || tags.size() == 0) && fileMeta.getTag() == null
          || Objects.equals(tags, fileMeta.getTag())) {
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
      if (tagFilter == null || TagKVUtils.match(fileMeta.getTag(), tagFilter)) {
        res.add(fi);
      }
    }
    return res.size() == 0 ? null : res;
  }

  /**
   * 根据提供的 tags 集合查找同名 / 近名的 .iginx 文件,其元数据包含 tags 中的所有 key-value 对。 若找到,返回包含这些文件的集合,否则返回 null。
   *
   * @param tags 用于匹配的 tags 集合
   * @param file 用于查找相同名或近名的父级目录
   * @return 包含 tags 中所有 key-value 对的 .iginx 文件集合,否则返回 null
   * @throws IOException 任何查找或读写操作导致的 IOException 将被传播
   */
  private static List<File> getFilesContainTag(File file, Map<String, String> tags)
      throws IOException {
    List<File> res = new ArrayList<>();
    List<File> files = getAssociatedFiles(file);

    if (files == null) return null;
    for (File f : files) {
      if (f.isDirectory()) continue;
      FileMeta fileMeta = fileOperator.getFileMeta(f);
      if (fileMeta.ifContainTag(tags)) {
        res.add(f);
      }
    }
    return res.size() == 0 ? null : res;
  }

  public static List<Pair<File, FileMeta>> getAllIginXFiles(File dir) {
    List<Pair<File, FileMeta>> res = new ArrayList<>();
    Stack<File> stack = new Stack<>();
    stack.push(dir);
    while (!stack.isEmpty()) {
      File current = stack.pop();
      List<File> fileList = null;
      if (current.isDirectory()) fileList = fileOperator.listFiles(current);
      else if (FileType.getFileType(current) == FileType.IGINX_FILE) {
        try {
          res.add(new Pair<>(current, fileOperator.getFileMeta(current)));
        } catch (IOException e) {
          logger.error(e.getMessage());
        }
      }
      if (fileList != null) {
        for (File file : fileList) {
          stack.push(file);
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
    Arrays.sort(files);
    return files[0];
  }

  static File getMaxFile(File dir) {
    File[] files = dir.listFiles();
    if (files.length == 0) {
      return dir;
    }
    Arrays.sort(files);
    File maxFile = files[files.length - 1];
    return maxFile;
  }

  public static Long getMaxTime(File dir) {
    List<Long> res = new ArrayList<>();
    List<File> files = getAssociatedFiles(dir);
    long max = Long.MIN_VALUE;

    for (File f : files) {
      long size = f.length();
      max = Math.max(max, size);
    }

    return max;
  }

  private long getFileSize(File file) {
    try {
      if (file.exists() && file.isFile()) {
        return fileOperator.length(file);
      }
    } catch (IOException e) {
      logger.error(e.getMessage());
    }

    return 0;
  }
}
