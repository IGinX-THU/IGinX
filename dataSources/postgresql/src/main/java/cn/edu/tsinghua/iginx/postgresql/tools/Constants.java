package cn.edu.tsinghua.iginx.postgresql.tools;

public class Constants {

    public static final String IGINX_SEPARATOR = ".";

    public static final String POSTGRESQL_SEPARATOR = "\u2E82";

    public static final int BATCH_SIZE = 10000;

    public static final String USERNAME = "username";

    public static final String PASSWORD = "password";

    public static final String DEFAULT_USERNAME = "postgres";

    public static final String DEFAULT_PASSWORD = "postgres";

    public static final String DATABASE_PREFIX = "unit";

    public static final String QUERY_DATABASES_STATEMENT = "SELECT datname FROM pg_database;";

    public static final String CREATE_DATABASE_STATEMENT = "CREATE DATABASE %s;";

    public static final String CONCAT_QUERY_STATEMENT = "SELECT concat(%s) FROM %s;";

    public static final String QUERY_STATEMENT = "SELECT time, %s FROM %s WHERE %s;";

    public static final String QUERY_STATEMENT_WITHOUT_WHERE_CLAUSE = "SELECT concat(%s) AS time, %s FROM %s;";

    public static final String CREATE_TABLE_STATEMENT = "CREATE TABLE %s (time BIGINT NOT NULL, %s %s, PRIMARY KEY(time));";

    public static final String ADD_COLUMN_STATEMENT = "ALTER TABLE %s ADD COLUMN %s %s;";

    public static final String DROP_DATABASE_STATEMENT = "DROP DATABASE %s;";

    public static final String DROP_COLUMN_STATEMENT = "ALTER TABLE %s DROP COLUMN IF EXISTS %s;";

    public static final String UPDATE_STATEMENT = "UPDATE %s SET %s = null WHERE (time >= %d AND time < %d);";
}
