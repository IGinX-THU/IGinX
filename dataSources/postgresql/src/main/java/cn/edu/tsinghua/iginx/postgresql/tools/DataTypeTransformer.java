package cn.edu.tsinghua.iginx.postgresql.tools;

import cn.edu.tsinghua.iginx.thrift.DataType;

import static cn.edu.tsinghua.iginx.thrift.DataType.*;

public class DataTypeTransformer {

    public static DataType fromPostgreSQL(String dataType) {
        if (dataType.equalsIgnoreCase("bool")) {
            return BOOLEAN;
        } else if (dataType.equalsIgnoreCase("int") || dataType.equalsIgnoreCase("int2") || dataType.equalsIgnoreCase("int4") ||
            dataType.equalsIgnoreCase("serial2") || dataType.equalsIgnoreCase("serial4")) {
            return INTEGER;
        } else if (dataType.equalsIgnoreCase("int8") || dataType.equalsIgnoreCase("serial8")) {
            return LONG;
        } else if (dataType.equalsIgnoreCase(("float4"))) {
            return FLOAT;
        } else if (dataType.equalsIgnoreCase("decimal") || dataType.equalsIgnoreCase("float8")) {
            return DOUBLE;
        } else {
            return BINARY;
        }
    }

    public static String toPostgreSQL(DataType dataType) {
        switch (dataType) {
            case BOOLEAN:
                return "BOOLEAN";
            case INTEGER:
                return "INTEGER";
            case LONG:
                return "BIGINT";
            case FLOAT:
                return "REAL";
            case DOUBLE:
                return "DOUBLE PRECISION";
            case BINARY:
            default:
                return "TEXT";
        }
    }
}
