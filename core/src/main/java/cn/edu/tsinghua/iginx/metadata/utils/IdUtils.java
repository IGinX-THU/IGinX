package cn.edu.tsinghua.iginx.metadata.utils;

import static cn.edu.tsinghua.iginx.conf.Constants.LENGTH_OF_SEQUENCE_NUMBER;

import cn.edu.tsinghua.iginx.conf.Constants;

public class IdUtils {

  public static String generateId(String prefix, long id) {
    return prefix + String.format("%0" + LENGTH_OF_SEQUENCE_NUMBER + "d", id);
  }

  public static String generateDummyStorageUnitId(long id) {
    return generateId(Constants.DUMMY, id);
  }
}
