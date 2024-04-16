package cn.edu.tsinghua.iginx.RelationAbstract.tools;

public abstract class Constants {
  public static final String TAGKV_EQUAL = "=";

  public static final String TAGKV_SEPARATOR = "-";

  public static final int BATCH_SIZE = 10000;

  public static final String USERNAME = "username";

  public static final String PASSWORD = "password";

  public static final String KEY_NAME = "postgresql+key";

  public static final String DATABASE_PREFIX = "unit";

  public static final String CREATE_DATABASE_STATEMENT = "CREATE DATABASE %s;";

  public static final String QUERY_STATEMENT =
      "SELECT \"" + KEY_NAME + "\", %s FROM %s %s ORDER BY \"" + KEY_NAME + "\";";

  public static final String QUERY_STATEMENT_WITHOUT_KEYNAME = "SELECT %s FROM %s %s ORDER BY %s;";

  public static final String CONCAT_QUERY_STATEMENT_AND_CONCAT_KEY =
      "SELECT concat(%s) AS \"" + KEY_NAME + "\", %s FROM %s %s ORDER BY concat(%s);";

  public static final String CREATE_TABLE_STATEMENT =
      "CREATE TABLE %s (\""
          + KEY_NAME
          + "\" BIGINT NOT NULL, %s %s, PRIMARY KEY(\""
          + KEY_NAME
          + "\"));";

  public static final String ADD_COLUMN_STATEMENT = "ALTER TABLE %s ADD COLUMN %s %s;";

  public static final String DROP_DATABASE_STATEMENT = "DROP DATABASE %s;";

  public static final String DROP_COLUMN_STATEMENT = "ALTER TABLE %s DROP COLUMN IF EXISTS %s;";

  public static final String UPDATE_STATEMENT =
      "UPDATE %s SET %s = null WHERE (\"" + KEY_NAME + "\" >= %d AND \"" + KEY_NAME + "\" < %d);";
}
