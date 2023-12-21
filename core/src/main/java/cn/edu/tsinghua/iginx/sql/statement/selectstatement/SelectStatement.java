package cn.edu.tsinghua.iginx.sql.statement.selectstatement;

import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.sql.statement.DataStatement;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;
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
  protected List<String> freeVariables;
  protected final List<String> orderByPaths;
  protected boolean ascending;
  protected int limit;
  protected int offset;
  private List<CommonTableExpression> cteList;

  public SelectStatement(boolean isSubQuery) {
    this.statementType = StatementType.SELECT;
    this.orderByPaths = new ArrayList<>();
    this.ascending = true;
    this.limit = Integer.MAX_VALUE;
    this.offset = 0;
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
    return orderByPaths;
  }

  public void setOrderByPath(String orderByPath) {
    this.orderByPaths.add(orderByPath);
  }

  public boolean isAscending() {
    return ascending;
  }

  public void setAscending(boolean ascending) {
    this.ascending = ascending;
  }

  public long getLimit() {
    return limit;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(int offset) {
    this.offset = offset;
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
