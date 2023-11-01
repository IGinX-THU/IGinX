package cn.edu.tsinghua.iginx.sql.statement.frompart;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.ALL_PATH_SUFFIX;

import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PathFromPart implements FromPart {

  private final String path;
  private JoinCondition joinCondition;
  private final String alias;

  public PathFromPart(String path) {
    this(path, "");
  }

  public PathFromPart(String path, String alias) {
    this.path = path;
    this.alias = alias;
  }

  @Override
  public String getOriginPrefix() {
    return path;
  }

  @Override
  public FromPartType getType() {
    return FromPartType.Path;
  }

  @Override
  public Map<String, String> getAliasMap() {
    Map<String, String> aliasMap = new HashMap<>();
    aliasMap.put(path + ALL_PATH_SUFFIX, alias + ALL_PATH_SUFFIX);
    return aliasMap;
  }

  @Override
  public boolean hasAlias() {
    return alias != null && !alias.isEmpty();
  }

  @Override
  public boolean hasSinglePrefix() {
    return true;
  }

  @Override
  public List<String> getPatterns() {
    return Collections.singletonList(getPrefix() + Constants.ALL_PATH_SUFFIX);
  }

  @Override
  public String getPrefix() {
    return hasAlias() ? alias : path;
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
