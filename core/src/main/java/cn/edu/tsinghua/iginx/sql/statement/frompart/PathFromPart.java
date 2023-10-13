package cn.edu.tsinghua.iginx.sql.statement.frompart;

import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;
import java.util.Collections;
import java.util.List;

public class PathFromPart implements FromPart {

  private final FromPartType type = FromPartType.PathFromPart;
  private final String path;
  private final boolean isJoinPart;
  private JoinCondition joinCondition;
  private final String alias;

  public PathFromPart(String path) {
    this(path, "");
  }

  public PathFromPart(String path, String alias) {
    this.path = path;
    this.isJoinPart = false;
    this.alias = alias;
  }

  public PathFromPart(String path, JoinCondition joinCondition) {
    this(path, joinCondition, "");
  }

  public PathFromPart(String path, JoinCondition joinCondition, String alias) {
    this.path = path;
    this.joinCondition = joinCondition;
    this.isJoinPart = true;
    this.alias = alias;
  }

  public String getOriginPrefix() {
    return path;
  }

  public String getAlias() {
    return alias;
  }

  public boolean hasAlias() {
    return alias != null && !alias.equals("");
  }

  @Override
  public FromPartType getType() {
    return type;
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
  public boolean isJoinPart() {
    return isJoinPart;
  }

  @Override
  public JoinCondition getJoinCondition() {
    return joinCondition;
  }

  @Override
  public List<String> getFreeVariables() {
    return Collections.emptyList();
  }
}
