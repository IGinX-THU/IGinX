/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.relational.meta;

import static cn.edu.tsinghua.iginx.relational.tools.Constants.KEY_NAME;

import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.relational.datatype.transformer.IDataTypeTransformer;
import java.util.List;

public abstract class AbstractRelationalMeta {

  protected StorageEngineMeta meta;

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
   * 获取通过concat生成key的query的SQL语句
   *
   * @return 通过concat生成key的query的SQL语句
   */
  public String getConcatQueryStatement() {
    return "SELECT %s AS " + getQuote() + KEY_NAME + getQuote() + ", %s FROM %s %s ORDER BY %s";
  }

  public abstract String getCreateTableStatement();

  public abstract String getDropDatabaseStatement();

  public abstract String getCreateDatabaseStatement();

  public abstract String getGrantPrivilegesStatement();

  public abstract String getAlterTableAddColumnStatement();

  public abstract String getAlterTableDropColumnStatement();

  public abstract String getQueryTableStatement();

  public abstract String getQueryTableWithoutKeyStatement();

  public abstract String getInsertTableStatement();

  public abstract String getUpdateTableStatement();

  public abstract String getDeleteTableStatement();

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

  /**
   * 获取不匹配正则表达式的操作符
   *
   * @return 不匹配正则表达式的操作符
   */
  public abstract String getNotRegexpOp();

  /** jdbc获取元数据是否支持反斜杠的识别 */
  public abstract boolean jdbcSupportSpecialChar();
}
