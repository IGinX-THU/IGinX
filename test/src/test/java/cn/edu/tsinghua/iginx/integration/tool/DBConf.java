package cn.edu.tsinghua.iginx.integration.tool;

import cn.edu.tsinghua.iginx.metadata.entity.*;
import java.util.HashMap;
import java.util.Map;

public final class DBConf {

  public enum DBConfType {
    isAbleToClearData,
    isAbleToDelete,
    isAbleToShowColumns,
    isSupportChinesePath,
    isSupportNumericalPath,
    isSupportSpecialCharacterPath
  }

  private static final Map<DBConfType, Boolean> DB_CONF_TYPE_MAP = new HashMap<>();

  private String StorageEngineMockConf = null;

  private String ClassName = null;

  static {
    // initial default value
    for (DBConfType type : DBConfType.values()) {
      DB_CONF_TYPE_MAP.put(type, true);
    }
  }

  public static DBConfType getDBConfType(String str) {
    switch (str) {
      case "isAbleToClearData":
        return DBConfType.isAbleToClearData;
      case "isAbleToDelete":
        return DBConfType.isAbleToDelete;
      case "isAbleToShowColumns":
        return DBConfType.isAbleToShowColumns;
      case "isSupportChinesePath":
        return DBConfType.isSupportChinesePath;
      case "isSupportNumericalPath":
        return DBConfType.isSupportNumericalPath;
      case "isSupportSpecialCharacterPath":
        return DBConfType.isSupportSpecialCharacterPath;
      default:
        throw new IllegalArgumentException("Invalid DBConfType value provided");
    }
  }

  public void setEnumValue(DBConfType dbConfType, boolean value) {
    DB_CONF_TYPE_MAP.put(dbConfType, value);
  }

  public boolean getEnumValue(DBConfType dbConfType) {
    return DB_CONF_TYPE_MAP.get(dbConfType);
  }

  public String getStorageEngineMockConf() {
    return StorageEngineMockConf;
  }

  public void setStorageEngineMockConf(String storageEngineMockConf) {
    StorageEngineMockConf = storageEngineMockConf;
  }

  public String getClassName() {
    return ClassName;
  }

  public void setClassName(String className) {
    ClassName = className;
  }
}
