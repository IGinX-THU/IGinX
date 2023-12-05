package cn.edu.tsinghua.iginx.filesystem.file;

import static cn.edu.tsinghua.iginx.filesystem.shared.Constant.*;
import static cn.edu.tsinghua.iginx.filesystem.shared.Constant.MAGIC_NUMBER;
import static cn.edu.tsinghua.iginx.utils.DataTypeUtils.transformObjectToStringByDataType;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import cn.edu.tsinghua.iginx.filesystem.file.entity.FileMeta;
import cn.edu.tsinghua.iginx.filesystem.query.entity.Record;
import cn.edu.tsinghua.iginx.filesystem.tools.LimitedSizeMap;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.DataTypeUtils;
import cn.edu.tsinghua.iginx.utils.JsonUtils;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultFileOperator implements IFileOperator {

  private static final Logger logger = LoggerFactory.getLogger(DefaultFileOperator.class);
  private LimitedSizeMap<File, BufferedWriter> appendWriterMap =
      new LimitedSizeMap<>(
          100_000,
          (BufferedWriter writer) -> {
            try {
              writer.close();
            } catch (IOException e) {
              e.printStackTrace();
            }
          });

  private LimitedSizeMap<File, Long> lastKeyMap = new LimitedSizeMap<>(100_000, null);

  public DefaultFileOperator() {}

  /**
   * Reads a range of bytes from a large file efficiently.
   *
   * @param file The file to read from.
   * @param readPos The starting byte position.
   * @return An array of bytes containing the read data.
   * @throws IOException If there is an error when reading the file.
   */
  @Override
  public byte[] readNormalFile(File file, long readPos, byte[] buffer) throws IOException {
    try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
      raf.seek(readPos);
      int len = raf.read(buffer);
      if (len < 0) {
        logger.info("reach the end of the file {} with len {}", file.getAbsolutePath(), len);
        return null;
      }
      if (len != buffer.length) {
        byte[] subBuffer;
        subBuffer = Arrays.copyOf(buffer, len);
        return subBuffer;
      }
      return buffer;
    } catch (IOException e) {
      throw new IOException(
          String.format(
              "readNormalFile %s failed with readPos:%d", file.getAbsoluteFile(), readPos),
          e);
    }
  }

  @Override
  public List<Record> readIginxFile(File file, long startKey, long endKey, DataType dataType)
      throws IOException {
    Exception e = flushAppendWriter(file);
    if (e != null) {
      throw new IOException(e);
    }
    List<Record> res = new ArrayList<>();
    long key;
    if (startKey == -1 && endKey == -1) {
      startKey = 0;
      endKey = Long.MAX_VALUE;
    }
    if (startKey < 0 || endKey < 0 || (startKey > endKey)) {
      throw new IllegalArgumentException(
          String.format(
              "readIginxFile %s failed with startKey:%d endKey:%d",
              file.getAbsoluteFile(), startKey, endKey));
    }

    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      String line;
      long currentLine = 0;
      while ((line = reader.readLine()) != null) {
        currentLine++;
        if (currentLine <= IGINX_FILE_META_INDEX) {
          continue;
        }
        String[] kv = line.split(",", 2);
        key = Long.parseLong(kv[0]);
        if (key >= startKey && key <= endKey) {
          res.add(
              new Record(
                  Long.parseLong(kv[0]),
                  dataType,
                  DataTypeUtils.parseStringByDataType(kv[1], dataType)));
        }
      }
    }
    return res;
  }

  private void test() {
    File file = new File(Paths.get("test", "test", "lhz", "debug.log").toUri());
    if (file.exists()) {
      int num = 400;
      File file1 = new File("test/filesystem/unit0000000000/us/d1/s1.iginx0");
      try (BufferedWriter tempWriter = new BufferedWriter(new FileWriter(file, true))) {
        tempWriter.write("begin\n");
        String lastLine = null;
        try (ReversedLinesFileReader reversedLinesReader = new ReversedLinesFileReader(file1, CHARSET)) {
          while ((lastLine = reversedLinesReader.readLine()) != null) {
            tempWriter.write(lastLine);
            tempWriter.write("\n");
            num--;
            if (num<=0) {
              break;
            }
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public Exception writeIginxFile(File file, List<Record> records) {
    logger.info("write iginx file {}", file.getAbsolutePath());
    logger.info("write iginx file {} with first {} ", file.getAbsolutePath(), records.get(0).getKey());
    if (file.exists() && file.isDirectory()) {
      return new IOException(String.format("cannot write to directory %s", file.getAbsolutePath()));
    }
    if (!file.exists()) {
      return new IOException(
          String.format(
              "cannot write to file %s because it does not exist", file.getAbsolutePath()));
    }

    // Check if records can be directly appended to the end of the file
    if (file.exists() && file.length() > 0) {
      long lastKey = getIginxFileMaxKey(file);
      if (lastKey == -1L || lastKey < records.get(0).getKey()) {
        appendRecordsToIginxFile(file, records, 0, records.size());
        return null;
      }
    }

    Exception exception = flushAppendWriter(file);
    if (exception != null) {
      return exception;
    }
    logger.info("flush writer for file {}", file.getAbsolutePath());

    // Create temporary file
    File tempFile = new File(file.getParentFile(), file.getName() + ".tmp");
    try (BufferedWriter tempWriter = new BufferedWriter(new FileWriter(tempFile))) {
      int recordIndex = 0;
      int maxLen = records.size();
      long minKey = records.get(0).getKey();
      long currentLine = 0L;

      try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
        String line;
        long key = -1L;
        while ((line = reader.readLine()) != null) {
          currentLine++;
          if (currentLine <= IGINX_FILE_META_INDEX) {
            tempWriter.write(line);
            tempWriter.write("\n");
            continue;
          }
          String[] kv = line.split(",", 2);
          key = Long.parseLong(kv[0]);
          boolean isCovered = false;
          // 找到了需要插入的位置
          while (key >= minKey && recordIndex < maxLen) {
            if (key == minKey) {
              isCovered = true;
            }
            Record record = records.get(recordIndex++);
            tempWriter.write(recordToString(record));
            tempWriter.write("\n");
            if (recordIndex < maxLen) {
              minKey = records.get(recordIndex).getKey();
            } else {
              break;
            }
          }
          if (!isCovered) {
            tempWriter.write(line);
            tempWriter.write("\n");
          }
        }
        updateLastKey(file, minKey);
        updateLastKey(file, key);
      }

      tempWriter.close();

      if (recordIndex < maxLen) {
        exception = appendRecordsToIginxFile(tempFile, records, recordIndex, records.size());
        updateLastKey(file, records.get(records.size() - 1).getKey());
        if (exception != null) {
          return exception;
        }
      }

      replaceFile(file, tempFile);
      return null;
    } catch (IOException e) {
      logger.error("write iginx file {} failure: {}", file.getAbsolutePath(), e.getMessage());
      return e;
    }
  }

  private boolean ifIginxFileEmpty(File file) {
    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      int lines = 0;
      while (reader.readLine() != null) {
        lines++;
        if (lines > IGINX_FILE_META_INDEX) {
          return false;
        }
      }
      return true;
    } catch (IOException e) {
      logger.error("cannot read file {} {}", file.getAbsolutePath(), e.getMessage());
      return false;
    }
  }

  // 直接将数据append到文件
  private Exception appendRecordsToIginxFile(File file, List<Record> records, int begin, int end) {
    BufferedWriter writer = null;
    if (!appendWriterMap.containsKey(file)) {
      try {
        writer = new BufferedWriter(new FileWriter(file, true), 262144);
        appendWriterMap.put(file, writer);
      } catch (IOException e) {
        logger.error("cannot create writer for file {} {}", file.getAbsolutePath(), e.getMessage());
        return e;
      }
    } else {
      writer = appendWriterMap.get(file);
    }

    try {
      for (int i = begin; i < end; i++) {
        writer.write(recordToString(records.get(i)));
        writer.write("\n");
      }
      updateLastKey(file, records.get(end - 1).getKey());
      return null;
    } catch (IOException e) {
      logger.error(
          "append records to iginx file {} failure: {}", file.getAbsolutePath(), e.getMessage());
      return e;
    }
  }

  private String recordToString(Record record) {
    return record.getKey()
        + ","
        + transformObjectToStringByDataType(record.getRawData(), record.getDataType());
  }

  // return -1表示空
  private long getIginxFileMaxKey(File file) {
    logger.info("get max key of iginx file {}", file.getAbsolutePath());
    if (lastKeyMap.containsKey(file)) {
      logger.info("get max key from map {}", lastKeyMap.get(file));
      return lastKeyMap.get(file);
    }
    try (ReversedLinesFileReader reversedLinesReader = new ReversedLinesFileReader(file, CHARSET)) {
      String lastLine = reversedLinesReader.readLine();
      if (!lastLine.contains(",")) {
        return -1L;
      }
      String line = lastLine.split(",", 2)[0]; // 获取第一个逗号之前的字符串
      try {
        long number = Long.parseLong(line); // 尝试将字符串解析为long类型
        logger.info("get max key from file {}", number);
        return number;
      } catch (NumberFormatException e) {
        logger.info("no data has been written to file {}", file.getAbsolutePath());
      }
      return -1L;
    } catch (IOException e) {
      logger.error(
          "get max key of iginx file {} failure: {}", file.getAbsolutePath(), e.getMessage());
      return -1L;
    }
  }

  private Exception replaceFile(File file, File tempFile) {
    if (!tempFile.exists()) {
      return new IOException(
          String.format("temporary file %s does not exist", tempFile.getAbsoluteFile()));
    }
    if (!file.exists()) {
      return new IOException(
          String.format("original file %s does not exist", file.getAbsoluteFile()));
    }
    Exception exception = closeAppendWriter(tempFile);
    if (exception != null) {
      return exception;
    }
    exception = closeAppendWriter(file);
    if (exception != null) {
      return exception;
    }
    try {
      Files.move(tempFile.toPath(), file.toPath(), REPLACE_EXISTING);
      return null;
    } catch (IOException e) {
      logger.error(
          "replace file from {} to {} failure: {}",
          tempFile.getAbsolutePath(),
          file.getAbsoluteFile(),
          e.getMessage());
      return e;
    }
  }

  @Override
  public File create(File file, FileMeta fileMeta) throws IOException {
    Path csvPath = Paths.get(file.getPath());
    if (!Files.exists(csvPath)) {
      file.getParentFile().mkdirs();
      Files.createFile(csvPath);
    } else {
      return file;
    }
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvPath.toFile()))) {
      for (int i = 1; i <= IGINX_FILE_META_INDEX; i++) {
        switch (i) {
          case MAGIC_NUMBER_INDEX:
            writer.write(new String(MAGIC_NUMBER));
            break;
          case DATA_TYPE_INDEX:
            writer.write(String.valueOf(fileMeta.getDataType().getValue()));
            break;
          case TAG_KV_INDEX:
            writer.write(new String(JsonUtils.toJson(fileMeta.getTags())));
            break;
        }
        writer.write("\n");
      }
    } catch (IOException e) {
      throw new IOException(String.format("cannot create file %s", file.getAbsolutePath()));
    }
    return file;
  }

  @Override
  public Exception delete(File file) {
    if (!file.exists()) {
      return new IOException(
          String.format("cannot delete file %s because it does not exist", file.getAbsolutePath()));
    }
    Exception exception = closeAppendWriter(file);
    if (exception != null) {
      return exception;
    }
    if (!file.delete()) {
      return new IOException(String.format("cannot delete file %s", file.getAbsolutePath()));
    }
    return null;
  }

  // 删除对应key范围内的数据
  @Override
  public Exception trimFile(File file, long begin, long end) {
    Exception exception = flushAppendWriter(file);
    if (exception != null) {
      return exception;
    }
    // Create temporary file
    File tempFile = new File(file.getParentFile(), file.getName() + ".tmp");
    try (BufferedWriter tempWriter = new BufferedWriter(new FileWriter(tempFile))) {
      long currentLine = 0L;

      try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
        String line;
        long lastKey = -1L;
        while ((line = reader.readLine()) != null) {
          currentLine++;
          if (currentLine <= IGINX_FILE_META_INDEX) {
            tempWriter.write(line);
            tempWriter.write("\n");
            continue;
          }
          String[] kv = line.split(",", 2);
          long key = Long.parseLong(kv[0]);
          if (key >= begin && key <= end) {
            continue;
          }
          lastKey = key;
          tempWriter.write(line);
          tempWriter.write("\n");
        }
        if (lastKey != -1L) {
          updateLastKey(file, lastKey);
        }
      }

      tempWriter.close();

      return replaceFile(file, tempFile);
    } catch (IOException e) {
      logger.error("trim file {} failure: {}", file.getAbsolutePath(), e.getMessage());
      return e;
    }
  }

  @Override
  public FileMeta getFileMeta(File file) throws IOException {
    Path csvPath = Paths.get(file.getPath());
    FileMeta fileMeta = new FileMeta();
    if (file.isDirectory()) {
      return fileMeta;
    }

    try {
      if (!Files.exists(csvPath)) {
        throw new IOException(
            String.format(
                "cannot get file meta %s because it does not exist", file.getAbsolutePath()));
      }

      try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
        int index = 1;
        String line;
        while ((line = reader.readLine()) != null && index <= IGINX_FILE_META_INDEX) {
          switch (index) {
            case MAGIC_NUMBER_INDEX:
              fileMeta.setMagicNumber(line.getBytes());
              break;
            case TAG_KV_INDEX:
              fileMeta.setTags(JsonUtils.parseMap(line, String.class, String.class));
              break;
            case DATA_TYPE_INDEX:
              fileMeta.setDataType(DataType.findByValue(Integer.parseInt(line)));
              break;
            default:
              break;
          }
          index++;
        }
      }
    } catch (IOException e) {
      throw new IOException(String.format("cannot get file meta %s", file.getAbsolutePath()));
    }
    return fileMeta;
  }

  @Override
  public List<File> listFiles(File file) {
    return listFiles(file, null);
  }

  @Override
  public List<File> listFiles(File file, String prefix) {
    FileFilter readFileFilter;
    if (prefix != null) {
      readFileFilter = f -> f.getName().startsWith(prefix) && !f.isHidden();
    } else {
      readFileFilter = f -> !f.isHidden();
    }

    File[] files;
    if (file.isDirectory()) {
      files = file.listFiles(readFileFilter);
    } else {
      files = file.getParentFile().listFiles(readFileFilter);
    }
    return files == null ? new ArrayList<>() : Arrays.asList(files);
  }

  private Exception closeAppendWriter(File file) {
    logger.info("close writer for file {}", file.getAbsolutePath());
    BufferedWriter writer = appendWriterMap.get(file);
    if (writer != null) {
      try {
        appendWriterMap.remove(file);
        writer.close();
      } catch (IOException e) {
        logger.error(
            "close writer for file {} failure: {}", file.getAbsolutePath(), e.getMessage());
        return e;
      }
    }
    return null;
  }

  private Exception flushAppendWriter(File file) {
    logger.info("flush writer for file {}", file.getAbsolutePath());
    BufferedWriter writer = appendWriterMap.get(file);
    if (writer != null) {
      try {
        writer.flush();
      } catch (IOException e) {
        logger.error(
            "flush writer for file {} failure: {}", file.getAbsolutePath(), e.getMessage());
        return e;
      }
    }
    return null;
  }

  private synchronized void updateLastKey(File file, long key) {
    logger.info("update last key for file {} with key {} and current max key {}", file.getAbsolutePath(), key, lastKeyMap.get(file));
    if (lastKeyMap.containsKey(file) && lastKeyMap.get(file) < key) {
      logger.info("last key for file {} is updated from {} to {}", file.getAbsolutePath(), lastKeyMap.get(file), key);
      lastKeyMap.put(file, key);
    } else if (!lastKeyMap.containsKey(file)) {
      logger.info("last key for file {} is updated from {} to {}", file.getAbsolutePath(), -1L, key);
      lastKeyMap.put(file, key);
    }
  }
}
