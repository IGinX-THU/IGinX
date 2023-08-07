package cn.edu.tsinghua.iginx.filesystem.file.entity;

import cn.edu.tsinghua.iginx.filesystem.file.IFileOperator;
import cn.edu.tsinghua.iginx.filesystem.file.property.FileMeta;
import cn.edu.tsinghua.iginx.filesystem.file.property.FileType;
import cn.edu.tsinghua.iginx.filesystem.tools.MemoryPool;
import cn.edu.tsinghua.iginx.filesystem.wrapper.Record;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.DataTypeUtils;
import cn.edu.tsinghua.iginx.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static cn.edu.tsinghua.iginx.thrift.DataType.BINARY;

public class DefaultFileOperator implements IFileOperator {
  private final int IGINX_FILE_PRE_READ_LEN = 8192;
  private static final Logger logger = LoggerFactory.getLogger(DefaultFileOperator.class);
  private final int BUFFER_SIZE = MemoryPool.getBlockSize();

  @Override
  public List<Record> readNormalFile(File file, long begin, long end, Charset charset)
      throws IOException {
    List<Record> res = new ArrayList<>();
    List<byte[]> valList = readNormalFileByByte(file, begin, end);
    long key = begin;
    for (byte[] val : valList) {
      res.add(new Record(key, val));
      key+=val.length;
    }
    return res;
  }

  /**
   * Reads a range of bytes from a large file efficiently.
   *
   * @param file The file to read from.
   * @param begin The starting byte position.
   * @param end The ending byte position.
   * @return An array of bytes containing the read data.
   * @throws IOException If there is an error reading the file.
   */
  public List<byte[]> readNormalFileByByte(File file, long begin, long end) throws IOException {
    if (file == null || !file.exists() || !file.isFile()) {
      throw new IllegalArgumentException("Invalid file.");
    }
    if (begin < 0 || end < begin) {
      throw new IllegalArgumentException("Invalid byte range.");
    }
    if (end > file.length()) {
      end = file.length() - 1;
    }
    ExecutorService executorService = null;
    List<Future<Void>> futures = new ArrayList<>();
    List<byte[]> res = new ArrayList<>();
    long size = end - begin;
    int round = (int) (size % BUFFER_SIZE == 0 ? size / BUFFER_SIZE : size / BUFFER_SIZE + 1);
    for (int i = 0; i < round; i++) {
      res.add(new byte[0]);
    }
    AtomicLong readPos = new AtomicLong(begin);
    AtomicInteger index = new AtomicInteger();
    int batchSize = BUFFER_SIZE;
    boolean ifNeedMultithread = file.length() / (BUFFER_SIZE) > 5;
    if (ifNeedMultithread) {
      executorService = Executors.newCachedThreadPool();
    }
    // Move the file pointer to the starting position
    try {
      while (readPos.get() < end) {
        long finalReadPos = readPos.get();
        int finalIndex = index.get();
        if (ifNeedMultithread) {
          futures.add(
              executorService.submit(
                  () -> {
                    readBatch(file, batchSize, finalReadPos, finalIndex, res);
                    return null;
                  }));
        } else {
          readBatch(file, batchSize, finalReadPos, finalIndex, res);
        }
        index.getAndIncrement();
        readPos.addAndGet(BUFFER_SIZE);
      }
      if (executorService != null) {
        executorService.shutdown();
        if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
          executorService.shutdownNow();
        }
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
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
        logger.error("Exception thrown by task: " + e.getMessage());
        throw new RuntimeException(e);
      }
    }
    return res;
  }

  // batchSize是读取的大小，readPos是开始读取的位置，index是该结果应该放在res中的第几个位置上
  public final void readBatch(File file, int batchSize, long readPos, int index, List<byte[]> res)
      throws IOException {
    try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
      byte[] buffer = MemoryPool.allocate(batchSize); // 一次读取1MB
      raf.seek(readPos);
      int len = raf.read(buffer);
      if (len < 0) {
        logger.info("reach the end of the file with len {}", len);
        return;
      }
      if (len != batchSize) {
        byte[] subBuffer;
        subBuffer = Arrays.copyOf(buffer, len);
        res.set(index, subBuffer);
      } else {
        res.set(index, buffer);
      }
    } catch (IOException e) {
      logger.error(
          "readBatch fail because {} with readPos:{} and index:{}", e.getMessage(), readPos, index);
      throw new IOException(e);
    }
  }

  // 获取iginx文件的meta信息，包括tag，以及存储的数据类型
  private Map<String, String> readIginxMetaInfo(File file) throws IOException {
    Map<String, String> result = new HashMap<>();
    BufferedReader reader = new BufferedReader(new FileReader(file));
    String line;
    int lineCount = 1;
    while ((line = reader.readLine()) != null) {
      if (lineCount == FileMeta.dataTypeIndex) {
        result.put(FileMeta.dataTypeName, line);
      } else if (lineCount == FileMeta.tagKVIndex) {
        result.put(FileMeta.tagKVName, line);
      }
      lineCount++;
    }
    reader.close();
    return result;
  }

  public List<Record> readIginxFileByKey(File file, long begin, long end, Charset charset)
      throws IOException {
    Map<String, String> fileInfo = readIginxMetaInfo(file);
    List<Record> res = new ArrayList<>();
    long key;
    if (begin == -1 && end == -1) {
      begin = 0;
      end = Long.MAX_VALUE;
    }
    if (begin < 0 || end < 0 || (begin > end)) {
      throw new IllegalArgumentException(
          "Read information outside the boundary with BEGIN " + begin + " and END " + end);
    }

    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      String line;
      long currentLine = 0;
      while ((line = reader.readLine()) != null) {
        currentLine++;
        if (currentLine <= FileMeta.iginxFileMetaIndex) {
          continue;
        }
        String[] kv = line.split(",", 2);
        key = Long.parseLong(kv[0]);
        if (key >= begin && key <= end) {
          DataType dataType = DataType.findByValue(Integer.parseInt(fileInfo.get(FileMeta.dataTypeName)));
          res.add(
              new Record(
                  Long.parseLong(kv[0]),
                  dataType,
                  DataTypeUtils.parseStringByDataTyp(kv[1], dataType)));
        }
      }
    }
    return res;
  }

  public List<Record> readDir(File file) throws IOException {
    return new ArrayList<>();
  }

  @Override
  public List<Record> readIGinXFileByKey(File file, long begin, long end, Charset charset)
      throws IOException {
    return readIginxFileByKey(file, begin, end, charset);
  }

  private String convertObjectToString(Object obj, DataType type) {
    if (obj == null) {
      return null;
    }

    if (type == null) {
      type = BINARY;
    }

    String strValue = null;
    try {
      switch (type) {
        case BINARY:
          strValue = new String((byte[]) obj);
          break;
        case INTEGER:
          strValue = Integer.toString((int) obj);
          break;
        case DOUBLE:
          strValue = Double.toString((double) obj);
          break;
        case FLOAT:
          strValue = Float.toString((float) obj);
          break;
        case BOOLEAN:
          strValue = Boolean.toString((boolean) obj);
          break;
        case LONG:
          strValue = Long.toString((long) obj);
          break;
        default:
          strValue = null;
          break;
      }
    } catch (Exception e) {
      strValue = null;
    }

    return strValue;
  }

  private final String recordToString(Record record) {
    return record.getKey() + "," + convertObjectToString(record.getRawData(), record.getDataType());
  }

  // 直接将数据append到文件
  private Exception appendValToIginxFile(File file, List<Record> valList, int begin, int end)
      throws IOException {
    if (begin == -1) begin = 0;
    if (end == -1) end = valList.size();
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, true))) {
      for (int i = begin; i < end; i++) {
        writer.write(recordToString(valList.get(i)));
        writer.write("\n");
      }
    }
    return null;
  }

  private Exception appendValToIginxFile(File file, List<Record> valList) throws IOException {
    return appendValToIginxFile(file, valList, -1, -1);
  }

  // 获取iginx文件中最后一行数据
  private String getLastValOfIginxFile(File file) throws IOException {
    String res = new String();
    int stepLen = IGINX_FILE_PRE_READ_LEN; // 8 KB
    if (ifIginxFileEmpty(file)) {
      return res;
    }
    // 一定包含数值
    if (file.exists() && file.length() > 0) {
      try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
        raf.seek(Math.max(file.length() - stepLen, 0));
        byte[] buffer = new byte[stepLen];
        int n = raf.read(buffer);
        String lastLine = new String();
        while (n != -1) {
          String data = new String(buffer, 0, n);
          String[] lines;
          lastLine += data;
          if (!lastLine.contains("\n")
              || lastLine.indexOf("\n") == lastLine.lastIndexOf("\n")) { // 是否确切包含了最后一行
            raf.seek(Math.max(raf.getFilePointer() - stepLen, 0));
            n = raf.read(buffer);
            continue;
          }
          lines = lastLine.split("\\r*\\n");
          return lines[lines.length - 1];
        }
      }
    }
    return res;
  }

  // return -1表示空
  private long getIginxFileMaxKey(File file) throws IOException {
    String lastLine = getLastValOfIginxFile(file);
    return Long.parseLong(lastLine.split(",", 2)[0]);
  }

  boolean ifIginxFileEmpty(File file) throws IOException {
    if (FileType.getFileType(file) != FileType.IGINX_FILE) {
      logger.error("not a iginx file!");
      return true;
    }

    boolean flag = true;
    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      int lines = 0;
      while (reader.readLine() != null) {
        lines++;
        if (lines > FileMeta.iginxFileMetaIndex) {
          flag = false;
          break;
        }
      }
      return flag;
    } catch (IOException e) {
      throw new IOException("Cannot get file: " + file.getAbsolutePath());
    }
  }

  private void replaceFile(File file, File tempFile) throws IOException {
    if (!tempFile.exists()) {
      throw new IOException("Temp file does not exist.");
    }
    if (!file.exists()) {
      throw new IOException("Original file does not exist.");
    }
    Files.move(tempFile.toPath(), file.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
  }

  @Override
  public Exception writeIGinXFile(File file, List<Record> valList) throws IOException {
    if (file.exists() && file.isDirectory()) {
      throw new IOException("Cannot write to directory: " + file.getAbsolutePath());
    }

    if (!file.exists()) {
      throw new IOException("Cannot write to file that not exist: " + file.getAbsolutePath());
    }

    // 如果是一个空文件，即没有内容，则直接添加数据
    if (ifIginxFileEmpty(file)) {
      return appendValToIginxFile(file, valList);
    }

    // Check if valList can be directly appended to the end of the file
    if (file.exists() && file.length() > 0) {
      long lastKey = getIginxFileMaxKey(file);
      if (lastKey < valList.get(0).getKey()) {
        return appendValToIginxFile(file, valList);
      }
    }

    // Create temporary file
    File tempFile = new File(file.getParentFile(), file.getName() + ".tmp");
    BufferedWriter writer = null;
    try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
      writer = new BufferedWriter(new FileWriter(tempFile));
      int valIndex = 0, maxLen = valList.size();
      long minKey = Math.min(valList.get(0).getKey(), Long.MAX_VALUE);
      long currentLine = 0L;
      BufferedReader reader = null;
      try {
        reader = new BufferedReader(new FileReader(file));
        String line;
        while ((line = reader.readLine()) != null) {
          currentLine++;
          if (currentLine <= FileMeta.iginxFileMetaIndex) {
            writer.write(line);
            writer.write("\n");
            continue;
          }
          String[] kv = line.split(",", 2);
          long key = Long.parseLong(kv[0]);
          boolean isCovered = false;
          // 找到了需要插入的位置
          while (key >= minKey && valIndex < maxLen) {
            if (key == minKey) isCovered = true;
            Record record = valList.get(valIndex++);
            writer.write(recordToString(record));
            writer.write("\n");
            if (valIndex < maxLen) {
              minKey = valList.get(valIndex).getKey();
            } else {
              break;
            }
          }
          if (!isCovered) {
            writer.write(line);
            writer.write("\n");
          }
        }
        writer.close();
        if (valIndex < maxLen) {
          appendValToIginxFile(tempFile, valList, valIndex, -1);
        }

      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    replaceFile(file, tempFile);
    return null;
  }

  // 删除对应key范围内的数据
  public Exception trimFile(File file, long begin, long end) throws IOException {
    // Create temporary file
    File tempFile = new File(file.getParentFile(), file.getName() + ".tmp");
    BufferedWriter writer = null;
    try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
      writer = new BufferedWriter(new FileWriter(tempFile));
      long currentLine = 0L;
      BufferedReader reader = null;
      try {
        reader = new BufferedReader(new FileReader(file));
        String line;
        while ((line = reader.readLine()) != null) {
          currentLine++;
          if (currentLine <= FileMeta.iginxFileMetaIndex) {
            writer.write(line);
            writer.write("\n");
            continue;
          }
          String[] kv = line.split(",", 2);
          long key = Long.parseLong(kv[0]);
          if (key >= begin && key <= end) {
            continue;
          }
          writer.write(line);
          writer.write("\n");
        }
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    replaceFile(file, tempFile);
    return null;
  }

  public boolean delete(File file) {
    if (!Files.exists(Paths.get(file.getPath()))) {
      logger.error("No file to delete: {}", file.getAbsolutePath());
      return false;
    }
    if (!file.delete()) {
      logger.error("Failed to delete file: {}", file.getAbsolutePath());
      return false;
    }
    return true;
  }

  @Override
  public File create(File file, FileMeta fileMeta) throws IOException {
    Path csvPath = Paths.get(file.getPath());

    try {
      if (!Files.exists(csvPath)) {
        file.getParentFile().mkdirs();
        Files.createFile(csvPath);
      } else {
        return file;
      }

      try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvPath.toFile()))) {
        writer.write(String.valueOf(fileMeta.getDataType().getValue()));
        writer.write("\n");
        writer.write(
            fileMeta.getTag() == null
                ? "{}"
                : new String(JsonUtils.toJson(fileMeta.getTag())));
        writer.write("\n");
        for (int i = 0; i < FileMeta.iginxFileMetaIndex - 3; i++) {
          writer.write("\n");
        }
      }

    } catch (IOException e) {
      throw new IOException("Cannot create file: " + file.getAbsolutePath());
    }
    return file;
  }

  public boolean mkdir(File file) {
    return file.mkdir();
  }

  public boolean isDirectory(File file) {
    return file.isDirectory();
  }

  @Override
  public FileMeta getFileMeta(File file) throws IOException {
    Path csvPath = Paths.get(file.getPath());
    FileMeta fileMeta = new FileMeta();
    if (file.isDirectory()) return fileMeta;

    try {
      if (!Files.exists(csvPath)) {
        logger.error("Cannot get file meta because not exist");
        throw new IOException("Cannot get file meta because not exist: " + file.getAbsolutePath());
      }

      try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
        int index = 1;
        String line;
        while ((line = reader.readLine()) != null && index <= FileMeta.iginxFileMetaIndex) {
          switch (index) {
            case FileMeta.tagKVIndex:
              fileMeta.setTag(JsonUtils.transformToSS(line));
              break;
            case FileMeta.dataTypeIndex:
              fileMeta.setDataType(DataType.findByValue(Integer.parseInt(line)));
              break;
            default:
              break;
          }
          index++;
        }
      }
    } catch (IOException e) {
      throw new IOException("Cannot get file meta : " + file.getAbsolutePath());
    }
    return fileMeta;
  }

  @Override
  public Boolean ifFileExists(File file) {
    Path path = Paths.get(file.getPath());
    return Files.exists(path);
  }

  @Override
  public List<File> listFiles(File file) {
    return listFiles(file, null);
  }

  @Override
  public List<File> listFiles(File file, String prefix) {
    FileFilter readFileFilter = null;
    if (prefix != null) {
      readFileFilter = file1 -> file1.getName().startsWith(prefix);
    }

    File[] files = null;
    if (file.isDirectory()) {
      files = file.listFiles(readFileFilter);
    } else {
      files = file.getParentFile().listFiles(readFileFilter);
    }
    return files == null || files.length == 0 ? null : Arrays.asList(files);
  }

  public long length(File file) throws IOException {
    if (FileType.getFileType(file) == FileType.IGINX_FILE) {
      return getIginxFileMaxKey(file);
    } else {
      return file.length();
    }
  }

  public boolean ifFilesEqual(File... file) {
    for (int i = 1; i < file.length; i++) {
      if (!file[i].getAbsolutePath().equals(file[i - 1].getAbsolutePath())) {
        return false;
      }
    }
    return true;
  }
}
