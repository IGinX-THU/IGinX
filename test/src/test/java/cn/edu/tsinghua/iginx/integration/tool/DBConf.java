package cn.edu.tsinghua.iginx.integration.tool;

import java.util.HashMap;
import java.util.Map;

public final class DBConf {
    public enum DBType {
        parquet,
        iotdb12,
        influxdb,
        mongodb,
        redis
    }

    public static DBType getDBType(String dbName) {
        switch (dbName.toLowerCase()) {
            case "iotdb12":
                return DBType.iotdb12;
            case "influxdb":
                return DBType.influxdb;
            case "parquet":
                return DBType.parquet;
            case "mongodb":
                return DBType.mongodb;
            case "redis":
                return DBType.redis;
            default:
                throw new IllegalArgumentException("Invalid DBName value provided");
        }
    }

    private static final Map<DBConfType, Boolean> DB_CONF_TYPE_MAP = new HashMap<>();

    public enum DBConfType {
        isAbleToClearData,
        isAbleToDelete,
        isAbleToShowTimeSeries,
        isSupportTagKV,
        isSupportChinesePath,
        isSupportNumericalPath,
        isSupportFileSystemSupportedSpecialPath,
        isSupportFileSystemNotSupportedSpecialPath
    }

    public static DBConfType getDBConfType(String str) {
        switch (str) {
            case "isAbleToClearData":
                return DBConfType.isAbleToClearData;
            case "isAbleToDelete":
                return DBConfType.isAbleToDelete;
            case "isAbleToShowTimeSeries":
                return DBConfType.isAbleToShowTimeSeries;
            case "isSupportTagKV":
                return DBConfType.isSupportTagKV;
            case "isSupportChinesePath":
                return DBConfType.isSupportChinesePath;
            case "isSupportNumericalPath":
                return DBConfType.isSupportNumericalPath;
            case "isSupportFileSystemSupportedSpecialPath":
                return DBConfType.isSupportFileSystemSupportedSpecialPath;
            case "isSupportFileSystemNotSupportedSpecialPath":
                return DBConfType.isSupportFileSystemNotSupportedSpecialPath;
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
}
