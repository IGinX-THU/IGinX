/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.sql.statement.select;

import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.sql.statement.DataStatement;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import cn.edu.tsinghua.iginx.sql.statement.select.subclause.LimitClause;
import cn.edu.tsinghua.iginx.sql.statement.select.subclause.OrderByClause;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class SelectStatement extends DataStatement {

  public static int markJoinCount = 0;
  protected SelectStatementType selectStatementType;
  protected boolean needLogicalExplain = false;
  protected boolean needPhysicalExplain = false;
  protected final boolean isSubQuery;
  private List<CommonTableExpression> cteList;

  protected List<String> freeVariables;

  protected LimitClause limitClause;

  protected OrderByClause orderByClause;

  public SelectStatement(boolean isSubQuery) {
    this.statementType = StatementType.SELECT;
    this.orderByClause = new OrderByClause();
    this.limitClause = new LimitClause();
    this.isSubQuery = isSubQuery;
    this.cteList = Collections.emptyList();
  }

  public SelectStatementType getSelectType() {
    return selectStatementType;
  }

  public boolean isNeedLogicalExplain() {
    return needLogicalExplain;
  }

  public void setNeedLogicalExplain(boolean needLogicalExplain) {
    this.needLogicalExplain = needLogicalExplain;
  }

  public boolean isNeedPhysicalExplain() {
    return needPhysicalExplain;
  }

  public void setNeedPhysicalExplain(boolean needPhysicalExplain) {
    this.needPhysicalExplain = needPhysicalExplain;
  }

  public boolean isSubQuery() {
    return isSubQuery;
  }

  public abstract List<Expression> getExpressions();

  public abstract Set<String> getPathSet();

  public List<String> getOrderByPaths() {
    return orderByClause.getOrderByPaths();
  }

  public void setOrderByPath(String orderByPath) {
    this.orderByClause.setOrderByPaths(orderByPath);
  }

  public boolean isAscending() {
    return orderByClause.isAscending();
  }

  public void setAscending(boolean ascending) {
    this.orderByClause.setAscending(ascending);
  }

  public long getLimit() {
    return limitClause.getLimit();
  }

  public void setLimit(int limit) {
    this.limitClause.setLimit(limit);
  }

  public long getOffset() {
    return limitClause.getOffset();
  }

  public void setOffset(int offset) {
    this.limitClause.setOffset(offset);
  }

  public List<CommonTableExpression> getCteList() {
    return cteList;
  }

  public void setCteList(List<CommonTableExpression> cteList) {
    this.cteList = cteList;
  }

  public List<String> getFreeVariables() {
    return freeVariables;
  }

  public void addFreeVariable(String freeVariable) {
    if (freeVariables == null) {
      this.freeVariables = new ArrayList<>();
    }
    this.freeVariables.add(freeVariable);
  }

  public abstract List<String> calculatePrefixSet();

  public abstract void initFreeVariables();

  public abstract Map<String, String> getSubQueryAliasMap(String alias);

  public enum SelectStatementType {
    UNARY,
    BINARY
  }
}
