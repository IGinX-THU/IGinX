package cn.edu.tsinghua.iginx.sql.statement.frompart;

import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;
import java.util.Collections;
import java.util.List;

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

  public String getOriginPrefix() {
    return path;
  }

  @Override
  public FromPartType getType() {
    return FromPartType.Path;
  }

  @Override
  public String getAlias() {
    return alias;
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
