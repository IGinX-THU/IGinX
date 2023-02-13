package cn.edu.tsinghua.iginx.engine.shared.function.udf.utils;

import cn.edu.tsinghua.iginx.thrift.DataType;

public class TypeUtils {

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
        } else if (object instanceof byte[]) {
            return DataType.BINARY;
        } else {
            return null;
        }
    }

    public static DataType getDataTypeFromTypeValue(Object object) {
        int value = ((Long) object).intValue();
        switch (value) {
            case 1:
                return DataType.BOOLEAN;
            case 2:
                return DataType.INTEGER;
            case 3:
                return DataType.LONG;
            case 4:
                return DataType.FLOAT;
            case 5:
                return DataType.DOUBLE;
            case 6:
                return DataType.BINARY;
            default:
                return null;
        }
    }

    public static Object getTypeValueFromDataType(DataType type) {
        switch (type) {
            case BOOLEAN:
                return 1;
            case INTEGER:
                return 2;
            case LONG:
                return 3;
            case FLOAT:
                return 4;
            case DOUBLE:
                return 5;
            case BINARY:
                return 6;
            default:
                return 0;
        }
    }

}
