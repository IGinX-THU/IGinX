package cn.edu.tsinghua.iginx.sql.statement.frompart;

import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.sql.statement.ShowColumnsStatement;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShowColumnsFromPart implements FromPart {

  private final ShowColumnsStatement showColumnsStatement;
  private final String alias;
  private JoinCondition joinCondition;

  public ShowColumnsFromPart(ShowColumnsStatement showColumnsStatement) {
    this(showColumnsStatement, "");
  }

  public ShowColumnsFromPart(ShowColumnsStatement showColumnsStatement, String alias) {
    this.showColumnsStatement = showColumnsStatement;
    this.alias = alias;
  }

  public ShowColumnsStatement getShowColumnsStatement() {
    return showColumnsStatement;
  }

  @Override
  public Map<String, String> getAliasMap() {
    Map<String, String> aliasMap = new HashMap<>();
    aliasMap.put("path", alias + ".path");
    aliasMap.put("type", alias + ".type");
    return aliasMap;
  }

  @Override
  public FromPartType getType() {
    return FromPartType.ShowColumns;
  }

  @Override
  public boolean hasAlias() {
    return alias != null && !alias.isEmpty();
  }

  @Override
  public boolean hasSinglePrefix() {
    return !alias.isEmpty();
  }

  @Override
  public List<String> getPatterns() {
    return hasAlias()
        ? Collections.singletonList(alias + Constants.ALL_PATH_SUFFIX)
        : new ArrayList<>(Arrays.asList("path", "type"));
  }

  @Override
  public String getOriginPrefix() {
    return "";
  }

  @Override
  public String getPrefix() {
    return hasAlias() ? alias : null;
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
