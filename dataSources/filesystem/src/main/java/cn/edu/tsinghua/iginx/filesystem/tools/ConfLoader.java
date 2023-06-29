package cn.edu.tsinghua.iginx.filesystem.tools;

import cn.edu.tsinghua.iginx.utils.ConfReader;
import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfLoader {
  private static final Logger logger = LoggerFactory.getLogger(ConfLoader.class);
  private static final String confPath =
      "dataSources/filesystem/src/main/resources/conf/config.properties";
  private static final String ROOT = "root";
  private static final String isLocal = "isLocalFileSystemStorage";
  public String ROOTPATH = null;

  public static String getRootPath() {
    String rowRootPath = ConfReader.getPropertyVal(confPath, ROOT);
    File rootFile = new File(rowRootPath);
    return rootFile.getAbsolutePath() + System.getProperty("file.separator");
  }

  public static File getRootFile() {
    String root = getRootPath();
    return new File(root);
  }

  public static boolean ifLocalFileSystem() {
    return Boolean.parseBoolean(ConfReader.getPropertyVal(confPath, isLocal));
  }
}
