package cn.edu.tsinghua.iginx.integration.tool;

public final class DBConf {
    public enum DBType {
        parquet,
        iotdb12,
        influxdb,
        mongodb
    }

    public static DBType getDBType(String dbName) {
        switch(dbName.toLowerCase()) {
            case "iotdb12":
                return DBType.iotdb12;
            case "influxdb":
                return DBType.influxdb;
            case "parquet":
                return DBType.parquet;
            case "mongodb":
                return DBType.mongodb;
            default:
                throw new IllegalArgumentException("Invalid DBName value provided");
        }
    }

    public enum DBConfType {
        isAbleToClearData,
        isAbleToDelete,
        isAbleToShowTimeSeries,
        isSupportSpecialPath,
        isSupportTagKV
    }

    private boolean ableToClearData;
    private boolean ableToDelete;
    private boolean ableToShowTimeSeries;
    private boolean supportSpecialPath;
    private boolean supportTagKV;

    public static DBConfType getDBConfType(String str) {
        switch(str) {
            case "isAbleToClearData":
                return DBConfType.isAbleToClearData;
            case "isAbleToDelete":
                return DBConfType.isAbleToDelete;
            case "isAbleToShowTimeSeries":
                return DBConfType.isAbleToShowTimeSeries;
            case "isSupportSpecialPath":
                return DBConfType.isSupportSpecialPath;
            case "isSupportTagKV":
                return DBConfType.isSupportTagKV;
            default:
                throw new IllegalArgumentException("Invalid DBConfType value provided");
        }
    }

    public void setEnumValue(DBConfType myEnum, boolean value) {
        switch(myEnum) {
            case isAbleToClearData:
                ableToClearData = value;
                break;
            case isAbleToDelete:
                ableToDelete = value;
                break;
            case isAbleToShowTimeSeries:
                ableToShowTimeSeries = value;
                break;
            case isSupportSpecialPath:
                supportSpecialPath = value;
                break;
            case isSupportTagKV:
                supportTagKV = value;
                break;
            default:
                throw new IllegalArgumentException("Invalid DBConfType value provided");
        }
    }

    public boolean getEnumValue(DBConfType myEnum) {
        switch(myEnum) {
            case isAbleToClearData:
                return ableToClearData;
            case isAbleToDelete:
                return ableToDelete;
            case isAbleToShowTimeSeries:
                return ableToShowTimeSeries;
            case isSupportSpecialPath:
                return supportSpecialPath;
            case isSupportTagKV:
                return supportTagKV;
            default:
                throw new IllegalArgumentException("Invalid DBConfType value provided");
        }
    }
}
