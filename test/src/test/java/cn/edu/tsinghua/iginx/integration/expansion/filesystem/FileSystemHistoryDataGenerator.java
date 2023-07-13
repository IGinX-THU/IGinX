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
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemHistoryDataGenerator extends BaseHistoryDataGenerator {
  private static final Logger logger =
      LoggerFactory.getLogger(FileSystemHistoryDataGenerator.class);
  private static String rootOri = "../dataSources/filesystem/src/test/java/cn/edu/tsinghua/iginx/";
  // 对应port 4860
  public static String root1 =
      rootOri + "storage/";
  // 对应port 4861
  public static String root2 =
      rootOri + "storage2/";
  public static String root3 =
      rootOri + "storage3/";

  public FileSystemHistoryDataGenerator() {
    this.oriPort = 4860;
    this.expPort = 4861;
    this.readOnlyPort = 4862;
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
      root = root1;
    } else if (port == expPort) {
      root = root2;
    } else {
      root = root3;
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

      try (OutputStream out = Files.newOutputStream(file.toPath())) {
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
}
