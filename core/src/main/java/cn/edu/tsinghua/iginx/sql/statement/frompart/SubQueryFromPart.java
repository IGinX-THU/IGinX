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
package cn.edu.tsinghua.iginx.sql.statement.frompart;

import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;
import cn.edu.tsinghua.iginx.sql.statement.select.BinarySelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.select.SelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.select.UnarySelectStatement;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.List;

public class SubQueryFromPart implements FromPart {

  private final SelectStatement subQuery;
  private final List<String> patterns;
  private JoinCondition joinCondition;
  private final String alias;

  public SubQueryFromPart(SelectStatement subQuery) {
    this(subQuery, "");
  }

  public SubQueryFromPart(SelectStatement subQuery, String alias) {
    this.subQuery = subQuery;
    this.patterns = subQuery.calculatePrefixSet();
    this.alias = alias;
  }

  public SubQueryFromPart(SelectStatement subQuery, JoinCondition joinCondition) {
    this(subQuery, joinCondition, "");
  }

  public SubQueryFromPart(SelectStatement subQuery, JoinCondition joinCondition, String alias) {
    this.subQuery = subQuery;
    this.patterns = subQuery.calculatePrefixSet();
    this.joinCondition = joinCondition;
    this.alias = alias;
  }

  @Override
  public FromPartType getType() {
    return FromPartType.SubQuery;
  }

  @Override
  public List<Pair<String, String>> getAliasList() {
    return subQuery.getSubQueryAliasList(alias);
  }

  @Override
  public boolean hasAlias() {
    return alias != null && !alias.isEmpty();
  }

  @Override
  public boolean hasSinglePrefix() {
    if (!subQuery.isSimpleQuery()) {
      return false;
    }

    if (hasAlias()) {
      return true;
    }

    SelectStatement s = subQuery;
    while (s.getSelectType().equals(SelectStatement.SelectStatementType.BINARY)) {
      s = ((BinarySelectStatement) s).getLeftQuery();
    }

    if (((UnarySelectStatement) s).getFromParts().size() > 1) {
      return false;
    }
    return ((UnarySelectStatement) s).getFromPart(0).hasSinglePrefix();
  }

  @Override
  public List<String> getPatterns() {
    return patterns;
  }

  @Override
  public String getOriginPrefix() {
    return "";
  }

  @Override
  public String getPrefix() {
    if (hasAlias()) {
      return alias;
    }
    // 如果子查询没有一个公共的前缀，返回null
    if (hasSinglePrefix()) {
      for (String pattern : patterns) {
        if (pattern.endsWith(Constants.ALL_PATH_SUFFIX)) {
          return pattern.substring(0, pattern.length() - 2);
        }
      }
    }
    return null;
  }

  @Override
  public JoinCondition getJoinCondition() {
    return joinCondition;
  }

  @Override
  public void setJoinCondition(JoinCondition joinCondition) {
    this.joinCondition = joinCondition;
  }

  public SelectStatement getSubQuery() {
    return subQuery;
  }

  @Override
  public List<String> getFreeVariables() {
    return subQuery.getFreeVariables();
  }
}
