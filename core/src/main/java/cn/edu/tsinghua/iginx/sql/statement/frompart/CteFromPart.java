package cn.edu.tsinghua.iginx.sql.statement.frompart;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.ALL_PATH_SUFFIX;
import static cn.edu.tsinghua.iginx.sql.SQLConstant.DOT;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;
import cn.edu.tsinghua.iginx.sql.statement.select.CommonTableExpression;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CteFromPart implements FromPart {

  private final CommonTableExpression cte;
  private JoinCondition joinCondition;
  private final String alias;

  public CteFromPart(CommonTableExpression cte) {
    this(cte, "");
  }

  public CteFromPart(CommonTableExpression cte, String alias) {
    this.cte = cte;
    this.alias = alias;
  }

  public Operator getRoot() {
    return cte.getRoot();
  }

  @Override
  public FromPartType getType() {
    return FromPartType.Cte;
  }

  @Override
  public boolean hasAlias() {
    return alias != null && !alias.isEmpty();
  }

  @Override
  public Map<String, String> getAliasMap() {
    Map<String, String> aliasMap = new HashMap<>();
    aliasMap.put(cte.getName() + ALL_PATH_SUFFIX, alias + ALL_PATH_SUFFIX);
    return aliasMap;
  }

  @Override
  public boolean hasSinglePrefix() {
    return true;
  }

  @Override
  public List<String> getPatterns() {
    if (cte.getColumns().isEmpty()) {
      return hasAlias()
          ? Collections.singletonList(alias + ALL_PATH_SUFFIX)
          : Collections.singletonList(cte.getName() + ALL_PATH_SUFFIX);
    } else {
      String prefix = getPrefix();
      List<String> patterns = new ArrayList<>();
      cte.getColumns().forEach(column -> patterns.add(prefix + DOT + column));
      return patterns;
    }
  }

  @Override
  public String getOriginPrefix() {
    return cte.getName();
  }

  @Override
  public String getPrefix() {
    return hasAlias() ? alias : cte.getName();
  }

  @Override
  public JoinCondition getJoinCondition() {
    return joinCondition;
  }

  @Override
  public void setJoinCondition(JoinCondition joinCondition) {
    this.joinCondition = joinCondition;
  }

  @Override
  public List<String> getFreeVariables() {
    return Collections.emptyList();
  }
}
