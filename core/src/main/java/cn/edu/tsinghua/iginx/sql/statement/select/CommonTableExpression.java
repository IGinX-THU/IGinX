package cn.edu.tsinghua.iginx.sql.statement.select;

import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.sql.SQLConstant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommonTableExpression {

  private final String name;

  private final List<String> columns;

  private final SelectStatement statement;

  private Operator root;

  public CommonTableExpression(SelectStatement statement, String name) {
    this(statement, name, Collections.emptyList());
  }

  public CommonTableExpression(SelectStatement statement, String name, List<String> columns) {
    this.name = name;
    this.columns = columns;
    this.statement = statement;
  }

  public String getName() {
    return name;
  }

  public List<String> getColumns() {
    return columns;
  }

  public SelectStatement getStatement() {
    return statement;
  }

  public Operator getRoot() {
    return root;
  }

  public void setRoot(Operator root) {
    this.root = root;
  }

  public Map<String, String> getAliasMap() {
    if (columns.isEmpty()) {
      return statement.getSubQueryAliasMap(name);
    } else {
      Map<String, String> aliasMap = new HashMap<>();
      for (int i = 0; i < columns.size(); i++) {
        Expression expression = statement.getExpressions().get(i);
        String originName =
            expression.hasAlias() ? expression.getAlias() : expression.getColumnName();
        aliasMap.put(originName, name + SQLConstant.DOT + columns.get(i));
      }
      return aliasMap;
    }
  }
}
