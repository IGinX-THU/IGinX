package cn.edu.tsinghua.iginx.relational.meta;

import static cn.edu.tsinghua.iginx.relational.tools.Constants.KEY_NAME;

import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.relational.datatype.transformer.IDataTypeTransformer;
import java.util.List;

public abstract class AbstractRelationalMeta {

  protected StorageEngineMeta meta;

  protected static final String DROP_DATABASE_STATEMENT = "DROP DATABASE IF EXISTS %s;";

  public AbstractRelationalMeta(StorageEngineMeta meta) {
    this.meta = meta;
  }

  /**
   * 获取ENGINE的默认数据库名称
   *
   * @return ENGINE的默认数据库名称
   */
  public abstract String getDefaultDatabaseName();

  /**
   * 获取ENGINE的驱动类
   *
   * @return ENGINE的驱动类
   */
  public abstract String getDriverClass();

  /**
   * 获取ENGINE的数据类型转换器
   *
   * @return ENGINE的数据类型转换器
   */
  public abstract IDataTypeTransformer getDataTypeTransformer();

  /**
   * 获取系统数据库名称，用于忽略，如pg的template0,template1
   *
   * @return 系统数据库名称
   */
  public abstract List<String> getSystemDatabaseName();

  /**
   * 获取数据库列表查询SQL
   *
   * @return 数据库列表查询SQL
   */
  public abstract String getDatabaseQuerySql();

  /**
   * 获取引号，在SQL中，不同的数据库引号不同，PG是双引号，MYSQL是反引号
   *
   * @return 引号
   */
  public abstract char getQuote();

  /**
   * 获取Update的SQL语句
   *
   * @return Update的SQL语句
   */
  public String getUpdateStatement() {
    return "UPDATE %s SET %s = null WHERE ("
        + getQuote()
        + KEY_NAME
        + getQuote()
        + " >= %d AND "
        + getQuote()
        + KEY_NAME
        + getQuote()
        + " < %d);";
  }

  /**
   * 获取query的SQL语句
   *
   * @return query的SQL语句
   */
  public String getQueryStatement() {
    return "SELECT "
        + getQuote()
        + KEY_NAME
        + getQuote()
        + ", %s FROM %s %s ORDER BY "
        + getQuote()
        + KEY_NAME
        + getQuote()
        + ";";
  }

  /**
   * 获取通过concat生成key的query的SQL语句
   *
   * @return 通过concat生成key的query的SQL语句
   */
  public String getConcatQueryStatement() {
    return "SELECT %s AS " + getQuote() + KEY_NAME + getQuote() + ", %s FROM %s %s ORDER BY %s";
  }

  public String getCreateTableStatement() {
    return "CREATE TABLE %s ("
        + getQuote()
        + KEY_NAME
        + getQuote()
        + " BIGINT NOT NULL, %s %s, PRIMARY KEY("
        + getQuote()
        + KEY_NAME
        + getQuote()
        + "));";
  }

  public abstract String getDropDatabaseStatement();

  /**
   * 在使用JDBC时元数据查询时，是否需要引号
   *
   * @return 是否需要引号
   */
  public abstract boolean jdbcNeedQuote();

  /**
   * 获取数据源在使用JDBC获取元数据时的schemaPattern
   *
   * @return schemaPattern
   */
  public abstract String getSchemaPattern();

  /**
   * 获取upsert中间那段SQL语句
   *
   * @return 获取upsert中间那段SQL语句
   */
  public abstract String getUpsertStatement();

  /**
   * 获取upsert冲突后段SQL语句格式
   *
   * @return 获取upsert冲突后段SQL语句格式
   */
  public abstract String getUpsertConflictStatement();

  /** 是否支持Full Join */
  public abstract boolean isSupportFullJoin();

  /**
   * 获取正则表达式的操作符
   *
   * @return 正则表达式的操作符
   */
  public abstract String getRegexpOp();

  /** jdbc获取元数据是否支持反斜杠的识别 */
  public abstract boolean jdbcSupportSpecialChar();
}
