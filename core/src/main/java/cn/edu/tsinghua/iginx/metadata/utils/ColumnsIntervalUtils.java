package cn.edu.tsinghua.iginx.metadata.utils;

import static cn.edu.tsinghua.iginx.utils.StringUtils.isContainSpecialChar;

import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnsIntervalUtils {

  public static final Logger logger = LoggerFactory.getLogger(ColumnsIntervalUtils.class);

  public static ColumnsInterval fromString(String str) throws IllegalArgumentException {
    if (str.contains("-") && !isContainSpecialChar(str)) {
      String[] parts = str.split("-");
      if (parts.length != 2) {
        logger.error("Input string {} in invalid format of ColumnsInterval ", str);
        throw new IllegalArgumentException("Input invalid string format in ColumnsInterval");
      }
      return new ColumnsInterval(
          parts[0].equals("null") ? null : parts[0], parts[1].equals("null") ? null : parts[1]);
    } else {
      if (str.contains(".*") && str.indexOf(".*") == str.length() - 2) {
        str = str.substring(0, str.length() - 2);
      }
      if (!isContainSpecialChar(str)) {
        return new ColumnsInterval(str);
      } else {
        logger.error("Input string {} in invalid format of ColumnsPrefixRange ", str);
        throw new IllegalArgumentException("Input invalid string format in ColumnsInterval");
      }
    }
  }
}
