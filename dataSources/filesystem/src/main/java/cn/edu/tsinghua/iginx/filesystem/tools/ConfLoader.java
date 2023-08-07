package cn.edu.tsinghua.iginx.filesystem.tools;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfLoader {
  private static final Logger logger = LoggerFactory.getLogger(ConfLoader.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final String confPath = "conf/config.properties";

  public static String getRootPath() {
    String rowRootPath = config.getMountedDirectory();
    File rootFile = new File(rowRootPath);
    return rootFile.getAbsolutePath() + System.getProperty("file.separator");
  }

  public static File getRootFile() {
    String root = getRootPath();
    return new File(root);
  }
}
