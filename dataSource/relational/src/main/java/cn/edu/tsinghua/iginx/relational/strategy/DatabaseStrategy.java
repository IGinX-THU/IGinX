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
package cn.edu.tsinghua.iginx.relational.strategy;

import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.zaxxer.hikari.HikariConfig;
import java.sql.*;
import java.util.List;
import java.util.Map;

public interface DatabaseStrategy {
  /**
   * 获取带引号的标识符名称
   *
   * @param name 需要加引号的原始名称
   * @return 添加引号后的名称
   */
  String getQuotName(String name);

  /**
   * 根据数据库名称和存储引擎元数据生成数据库连接URL
   *
   * @param databaseName 数据库名称
   * @param meta 存储引擎元数据
   * @return 完整的数据库连接URL
   */
  String getUrl(String databaseName, StorageEngineMeta meta);

  /**
   * 根据存储引擎元数据生成基本连接URL
   *
   * @param meta 存储引擎元数据
   * @return 基本连接URL
   */
  String getConnectUrl(StorageEngineMeta meta);

  void configureDataSource(HikariConfig config, String databaseName, StorageEngineMeta meta);

  /**
   * 从数据库查询结果集中提取数据库名称
   *
   * @param rs 数据库查询结果集
   * @return 数据库名称
   * @throws SQLException 如果访问结果集时发生SQL异常
   */
  String getDatabaseNameFromResultSet(ResultSet rs) throws SQLException;

  String getDatabasePattern(String databaseName, boolean isDummy);

  /**
   * 获取用于查询数据库模式的模式模式字符串
   *
   * @param databaseName 数据库名称
   * @return 模式模式字符串
   */
  String getSchemaPattern(String databaseName, boolean isDummy);

  /**
   * 格式化SQL连接语句，用于将多个列合并成一个表达式
   *
   * @param columns 需要连接的列名列表
   * @return 格式化后的连接表达式
   */
  String formatConcatStatement(List<String> columns);

  /**
   * 执行批量数据插入操作（Upsert）
   *
   * @param conn 数据库连接
   * @param databaseName 目标数据库名称
   * @param stmt SQL语句对象
   * @param tableToColumnEntries 表名到列信息的映射，包含表名、主键和列名列表
   * @param quote 标识符引用字符
   * @throws SQLException 如果执行批量插入时发生SQL异常
   */
  void executeBatchInsert(
      Connection conn,
      String databaseName,
      Statement stmt,
      Map<String, Pair<String, List<String>>> tableToColumnEntries,
      char quote)
      throws SQLException;

  /**
   * 获取用于平均值计算的类型转换表达式
   *
   * @param param 需要计算平均值的表达式，可能是一个列名或其他表达式
   * @return 包含类型转换的平均值表达式
   */
  String getAvgCastExpression(Expression param);
}
