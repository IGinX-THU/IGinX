package cn.edu.tsinghua.iginx.filesystem.constant;

public class Constant {

  public static final String SEPARATOR = System.getProperty("file.separator");

  public static final String FILE_EXTENSION = ".iginx";

  public static final String WILDCARD = "*";

  public static final String FILE_PATH_FORMAT = "%s%s" + SEPARATOR + "%s.iginx";

  public static final String DIR_PATH_FORMAT = "%s%s" + SEPARATOR + "%s" + SEPARATOR;
}
