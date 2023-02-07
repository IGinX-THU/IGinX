package cn.edu.tsinghua.iginx.parquet.tools;

public class Constant {

    public static final String NAME = "name";

    public static final String COLUMN_NAME = "column_name";

    public static final String COLUMN_TYPE = "type";

    public static final String COLUMN_TIME = "time";

    public static final String DATATYPE_BIGINT = "BIGINT";

    public static final String DUCKDB_SCHEMA = "duckdb_schema";

    public static final String IGINX_SEPARATOR = "\\.";

    public static final String PARQUET_SEPARATOR = "\\$";

    public static final String SUFFIX_PARQUET_FILE = ".parquet";

    public static final String SUFFIX_EXTRA_FILE = ".extra";

    public static final String CMD_PATHS = "PATHS";

    public static final String CMD_TIME = "TIME";

    public static final String CMD_DELETE = "DELETE";

    public static final int MAX_MEM_SIZE = 100 * 1024 * 1024 /* BYTE */;

    public static final String CREATE_TABLE_STMT = "CREATE TABLE %s (%s)";

    public static final String INSERT_STMT_PREFIX = "INSERT INTO %s(%s) VALUES ";

    public static final String CREATE_TABLE_FROM_PARQUET_STMT = "CREATE TABLE %s AS SELECT * FROM '%s'";

    public static final String ADD_COLUMNS_STMT = "ALTER TABLE %s ADD COLUMN %s %s";

    public static final String DESCRIBE_STMT = "DESCRIBE %s";

    public static final String SAVE_TO_PARQUET_STMT = "COPY %s TO '%s' (FORMAT 'parquet')";

    public static final String DROP_TABLE_STMT = "DROP TABLE %s";

    public static final String SELECT_STMT = "SELECT time, %s FROM '%s' WHERE %s ORDER BY time";

    public static final String SELECT_MEM_STMT = "SELECT time, %s FROM %s WHERE %s ORDER BY time";

    public static final String SELECT_TIME_STMT = "SELECT time FROM '%s' ORDER BY time";

    public static final String SELECT_FIRST_TIME_STMT = "SELECT time FROM '%s' order by time limit 1";

    public static final String SELECT_LAST_TIME_STMT = "SELECT time FROM '%s' order by time desc limit 1";

    public static final String SELECT_PARQUET_SCHEMA = "SELECT * FROM parquet_schema('%s')";

    public static final String DELETE_DATA_STMT = "UPDATE %s SET %s=NULL WHERE time >= %s AND time <= %s";

    public static final String DROP_COLUMN_STMT = "ALTER TABLE %s DROP %s";

}
