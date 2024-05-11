package cn.edu.tsinghua.iginx.relational.tools;

import java.util.HashMap;
import java.util.Map;

public abstract class Constants {
  public static final String TAGKV_EQUAL = "=";

  public static final String TAGKV_SEPARATOR = "-";

  public static final int BATCH_SIZE = 10000;

  public static final String USERNAME = "username";

  public static final String PASSWORD = "password";

  public static final String KEY_NAME = "RELATIONAL+KEY";

  public static final String DATABASE_PREFIX = "unit";

  public static final String CREATE_DATABASE_STATEMENT = "CREATE DATABASE %s;";

  public static final String QUERY_STATEMENT_WITHOUT_KEYNAME = "SELECT %s FROM %s %s ORDER BY %s;";

  public static final String ADD_COLUMN_STATEMENT = "ALTER TABLE %s ADD COLUMN %s %s;";

  public static final String DROP_COLUMN_STATEMENT = "ALTER TABLE %s DROP COLUMN %s;";

  public static final Map<String, String> classMap = new HashMap<>();

  static {
    classMap.put("postgresql", "cn.edu.tsinghua.iginx.relational.meta.PostgreSQLMeta");
  }
}
