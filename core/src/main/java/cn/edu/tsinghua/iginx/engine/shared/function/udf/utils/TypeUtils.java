package cn.edu.tsinghua.iginx.engine.shared.function.udf.utils;

import cn.edu.tsinghua.iginx.thrift.DataType;

public class TypeUtils {

    public static DataType getDataTypeFromString(String type) {
        switch (type.toLowerCase()) {
            case "boolean":
                return DataType.BOOLEAN;
            case "integer":
                return DataType.INTEGER;
            case "long":
                return DataType.LONG;
            case "float":
                return DataType.FLOAT;
            case "double":
                return DataType.DOUBLE;
            case "binary":
                return DataType.BINARY;
            default:
                return null;
        }
    }

    public static DataType getDataTypeFromObject(Object object) {
        if (object instanceof Boolean) {
            return DataType.BOOLEAN;
        } else if (object instanceof Integer) {
            return DataType.INTEGER;
        } else if (object instanceof Long) {
            return DataType.LONG;
        } else if (object instanceof Float) {
            return DataType.FLOAT;
        } else if (object instanceof Double) {
            return DataType.DOUBLE;
        } else if (object instanceof String) {
            return DataType.BINARY;
        } else if (object instanceof byte[]) {
            return DataType.BINARY;
        } else {
            return null;
        }
    }
}
