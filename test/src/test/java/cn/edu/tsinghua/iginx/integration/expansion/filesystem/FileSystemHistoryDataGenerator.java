package cn.edu.tsinghua.iginx.integration.expansion.filesystem;

import cn.edu.tsinghua.iginx.filesystem.file.property.FilePath;
import cn.edu.tsinghua.iginx.filesystem.tools.ConfLoader;
import cn.edu.tsinghua.iginx.filesystem.tools.MemoryPool;
import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemHistoryDataGenerator extends BaseHistoryDataGenerator {
  private static final Logger logger =
      LoggerFactory.getLogger(FileSystemHistoryDataGenerator.class);
  public static String rootTest = "../dataSources/filesystem/src/test/java/cn/edu/tsinghua/iginx/";
  public static String rootAct = "dataSources/filesystem/src/test/java/cn/edu/tsinghua/iginx/";
  // 对应port 4860
  public static String root1 = "storage/";
  // 对应port 4861
  public static String root2 = "storage2/";
  public static String root3 = "storage3/";

  public FileSystemHistoryDataGenerator() {
    this.oriPort = 4860;
    this.expPort = 4861;
    this.readOnlyPort = 4862;
    EXP_DATA_TYPE_LIST = Arrays.asList(DataType.BINARY, DataType.BINARY);
    byte[] value1 = createValueRandom(1),
        value2 = createValueRandom(2),
        value3 = createValueRandom(3),
        value4 = createValueRandom(4);
    EXP_VALUES_LIST =
        Arrays.asList(
            Arrays.asList(value1, value2),
            Arrays.asList(value3, value4));
    ORI_DATA_TYPE_LIST = Arrays.asList(DataType.BINARY, DataType.BINARY);
    ORI_VALUES_LIST =
        Arrays.asList(
            Arrays.asList(value1, value2),
            Arrays.asList(value3, value4));
    READ_ONLY_DATA_TYPE_LIST = Arrays.asList(DataType.BINARY, DataType.BINARY);
    READ_ONLY_VALUES_LIST =
        Arrays.asList(
            Arrays.asList(value1, value2),
            Arrays.asList(value3, value4));
  }

  public void deleteDirectory(String path) {
    File directory = new File(path);

    // 如果目录不存在,什么也不做
    if (!directory.exists()) return;

    for (File file : directory.listFiles()) {
      // 如果是文件,删除它
      if (file.isFile()) {
        file.delete();
      } else if (file.isDirectory()) {
        // 如果是目录,先删除里面所有的内容
        deleteDirectory(file.getPath());
        // 再删除外层目录
        file.delete();
      }
    }
    directory.delete();
  }

  @Override
  public void writeHistoryData(
      int port, List<String> pathList, List<DataType> dataTypeList, List<List<Object>> valuesList) {
    String root = getRootFromPort(port);
    // 创建文件
    List<File> files = getFileList(pathList, root);
    // 将数据写入
    writeValuesToFile(valuesList, files);
  }

  @Override
  public void clearHistoryDataForGivenPort(int port) {
    String root = getRootFromPort(port);
    deleteDirectory(root);
  }

  public String getRootFromPort(int port) {
    String root = null;
    if (port == oriPort) {
      root = rootTest + root1;
    } else if (port == expPort) {
      root = rootTest + root2;
    } else {
      root = rootTest + root3;
    }
    return root;
  }



  public List<File> getFileList(List<String> pathList, String root) {
    List<File> res = new ArrayList<>();
    // 创建历史文件
    for (String path : pathList) {
      String realFilePath = root + path.replace('.','/');
      File file = new File(realFilePath);
      res.add(file);
      Path filePath = Paths.get(file.getPath());
      try {
        if (!Files.exists(filePath)) {
          file.getParentFile().mkdirs();
          Files.createFile(filePath);
          logger.info("create the file {}", file.getAbsolutePath());
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return res;
  }

  public void writeValuesToFile(List<List<Object>> valuesList, List<File> files) {
    if (valuesList.size() != files.size()) {
      throw new IllegalArgumentException("Number of values lists and files don't match");
    }

    int numFiles = files.size();
    for (int i = 0; i < numFiles; i++) {
      List<Object> values = valuesList.get(i);
      File file = files.get(i);

      try (OutputStream out = Files.newOutputStream(file.toPath(), StandardOpenOption.APPEND)) {
        for (Object value : values) {
          if (value instanceof byte[]) {
            out.write((byte[])value);
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static byte[] createValueRandom(int seed) {
    int N = MemoryPool.getBlockSize();
    byte[] b = new byte[N];
    Random random = new Random(seed);
    random.nextBytes(b);
    return b;
  }
}
