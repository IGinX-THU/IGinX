package cn.edu.tsinghua.iginx.postgresql.tools;

import static cn.edu.tsinghua.iginx.thrift.DataType.BINARY;
import static cn.edu.tsinghua.iginx.thrift.DataType.BOOLEAN;
import static cn.edu.tsinghua.iginx.thrift.DataType.DOUBLE;
import static cn.edu.tsinghua.iginx.thrift.DataType.LONG;

import cn.edu.tsinghua.iginx.thrift.DataType;

public class DataTypeTransformer {

    public static DataType fromPostgreSQL(String dataType) {
        if (dataType.contains("int")
                || dataType.contains("timestamptz")
                || dataType.contains("serial")) {
            return LONG;
        } else if (dataType.contains("bool")) {
            return BOOLEAN;
        } else if (dataType.contains("float")) {
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
