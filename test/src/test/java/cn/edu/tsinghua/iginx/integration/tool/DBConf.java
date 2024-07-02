/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.integration.tool;

import java.util.HashMap;
import java.util.Map;

public final class DBConf {

  public enum DBConfType {
    isAbleToClearData,
    isAbleToDelete,
    isAbleToShowColumns,
    isSupportChinesePath,
    isSupportNumericalPath,
    isSupportSpecialCharacterPath,
    isSupportKey,
    isSupportDiffTypeHistoryData
  }

  private static final Map<DBConfType, Boolean> DB_CONF_TYPE_MAP = new HashMap<>();

  // for dbce port map
  public static final Map<String, Integer> DBCE_PORT_MAP = new HashMap<>();

  private String storageEngineMockConf = null;

  private String className = null;

  private String historyDataGenClassName = null;

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
      case "isSupportKey":
        return DBConfType.isSupportKey;
      case "isSupportDiffTypeHistoryData":
        return DBConfType.isSupportDiffTypeHistoryData;
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
    return storageEngineMockConf;
  }

  public void setStorageEngineMockConf(String storageEngineMockConf) {
    this.storageEngineMockConf = storageEngineMockConf;
  }

  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public String getHistoryDataGenClassName() {
    return historyDataGenClassName;
  }

  public void setHistoryDataGenClassName(String historyDataGenClassName) {
    this.historyDataGenClassName = historyDataGenClassName;
  }

  public void setDbcePortMap(String portName, int port) {
    DBCE_PORT_MAP.put(portName, port);
  }

  public Map<String, Integer> getDBCEPortMap() {
    return DBCE_PORT_MAP;
  }
}
