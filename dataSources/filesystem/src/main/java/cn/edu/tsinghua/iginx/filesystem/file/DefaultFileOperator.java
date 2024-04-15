package cn.edu.tsinghua.iginx.filesystem.file;

import static cn.edu.tsinghua.iginx.filesystem.shared.Constant.*;
import static cn.edu.tsinghua.iginx.filesystem.shared.Constant.MAGIC_NUMBER;
import static cn.edu.tsinghua.iginx.utils.DataTypeUtils.transformObjectToStringByDataType;

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
import java.nio.file.StandardCopyOption;
import java.util.*;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultFileOperator implements IFileOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultFileOperator.class);
  private LimitedSizeMap<File, BufferedWriter> appendWriterMap =
      new LimitedSizeMap<>(
          100_000,
          (BufferedWriter writer) -> {
            try {
              writer.close();
            } catch (IOException e) {
              throw new RuntimeException("close writer failed", e);
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
        LOGGER.info("reach the end of the file {} with len {}", file.getAbsolutePath(), len);
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
    flushAppendWriter(file);
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

  @Override
  public void writeIginxFile(File file, List<Record> records) throws IOException {
    if (file.exists() && file.isDirectory()) {
      throw new IOException(String.format("cannot write to directory %s", file.getAbsolutePath()));
    }
    if (!file.exists()) {
      throw new IOException(
          String.format(
              "cannot write to file %s because it does not exist", file.getAbsolutePath()));
    }

    // Check if records can be directly appended to the end of the file
    if (file.exists() && file.length() > 0) {
      long lastKey = getIginxFileMaxKey(file);
      if (lastKey == -1L || lastKey < records.get(0).getKey()) {
        appendRecordsToIginxFile(file, records, 0, records.size());
      }
    }

    flushAppendWriter(file);

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
        appendRecordsToIginxFile(tempFile, records, recordIndex, records.size());
        updateLastKey(file, records.get(records.size() - 1).getKey());
      }

      replaceFile(file, tempFile);
    } catch (IOException e) {
      throw new IOException(
          String.format("write iginx file %s failure", file.getAbsolutePath()), e);
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
      LOGGER.error("cannot read file {} ", file.getAbsolutePath(), e);
      return false;
    }
  }

  // 直接将数据append到文件
  private void appendRecordsToIginxFile(File file, List<Record> records, int begin, int end)
      throws IOException {
    BufferedWriter writer = null;
    if (!appendWriterMap.containsKey(file)) {
      try {
        writer = new BufferedWriter(new FileWriter(file, true), 262144);
        appendWriterMap.put(file, writer);
      } catch (IOException e) {
        throw new IOException(
            String.format("cannot create writer for file %s", file.getAbsolutePath()), e);
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
    } catch (IOException e) {
      throw new IOException(
          String.format("append records to iginx file %s failure", file.getAbsolutePath()), e);
    }
  }

  private String recordToString(Record record) {
    return record.getKey()
        + ","
        + transformObjectToStringByDataType(record.getRawData(), record.getDataType());
  }

  // return -1表示空
  private long getIginxFileMaxKey(File file) {
    if (lastKeyMap.containsKey(file)) {
      return lastKeyMap.get(file);
    }
    try (ReversedLinesFileReader reversedLinesReader = new ReversedLinesFileReader(file, CHARSET)) {
      String lastLine = reversedLinesReader.readLine();
      if (!lastLine.contains(",")) {
        return -1L;
      }
      String line = lastLine.substring(0, lastLine.indexOf(",")); // 获取第一个逗号之前的字符串
      try {
        long number = Long.parseLong(line); // 尝试将字符串解析为long类型
        return number;
      } catch (NumberFormatException e) {
        LOGGER.info("no data has been written to file {}", file.getAbsolutePath());
      }
      return -1L;
    } catch (IOException e) {
      LOGGER.error("get max key of iginx file {} failure: ", file.getAbsolutePath(), e);
      return -1L;
    }
  }

  private void replaceFile(File file, File tempFile) throws IOException {
    if (!tempFile.exists()) {
      throw new IOException(
          String.format("temporary file %s does not exist", tempFile.getAbsoluteFile()));
    }
    if (!file.exists()) {
      throw new IOException(
          String.format("original file %s does not exist", file.getAbsoluteFile()));
    }
    try {
      moveFileWithLock(tempFile, file);
    } catch (IOException e) {
      throw new IOException(
          String.format(
              "replace file from %s to %s failure",
              tempFile.getAbsolutePath(), file.getAbsoluteFile()),
          e);
    }
  }

  private void moveFileWithLock(File source, File target) throws IOException {
    boolean movedSuccessfully = false;

    while (!movedSuccessfully) {
      closeAppendWriter(target);
      closeAppendWriter(source);
      try {
        Files.move(source.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
        movedSuccessfully = true;
      } catch (Exception e) {
        LOGGER.error(
            "move file from {} to {} failure and wait",
            source.getAbsolutePath(),
            target.getAbsoluteFile(),
            e);
        try {
          Thread.sleep(1);
        } catch (InterruptedException interruptedException) {
          interruptedException.printStackTrace();
        }
      }
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
  public void delete(File file) throws IOException {
    if (!file.exists()) {
      throw new IOException(
          String.format("cannot delete file %s because it does not exist", file.getAbsolutePath()));
    }
    closeAppendWriter(file);
    if (!file.delete()) {
      throw new IOException(String.format("cannot delete file %s", file.getAbsolutePath()));
    }
  }

  // 删除对应key范围内的数据
  @Override
  public void trimFile(File file, long begin, long end) throws IOException {
    flushAppendWriter(file);
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

      replaceFile(file, tempFile);
    } catch (IOException e) {
      throw new IOException(String.format("trim file %s failure", file.getAbsolutePath()), e);
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

  private void closeAppendWriter(File file) throws IOException {
    LOGGER.debug("close writer for file {}", file.getAbsolutePath());
    BufferedWriter writer = appendWriterMap.get(file);
    if (writer != null) {
      try {
        appendWriterMap.remove(file);
        writer.close();
      } catch (IOException e) {
        throw new IOException(
            String.format("close writer for file %s failure", file.getAbsolutePath()), e);
      }
    }
  }

  private void flushAppendWriter(File file) throws IOException {
    LOGGER.debug("flush writer for file {}", file.getAbsolutePath());
    BufferedWriter writer = appendWriterMap.get(file);
    if (writer != null) {
      try {
        writer.flush();
      } catch (IOException e) {
        throw new IOException(
            String.format("flush writer for file %s failure", file.getAbsolutePath()), e);
      }
    }
  }

  private void updateLastKey(File file, long key) {
    if (lastKeyMap.containsKey(file) && lastKeyMap.get(file) < key) {
      lastKeyMap.put(file, key);
    } else if (!lastKeyMap.containsKey(file)) {
      lastKeyMap.put(file, key);
    }
  }
}
