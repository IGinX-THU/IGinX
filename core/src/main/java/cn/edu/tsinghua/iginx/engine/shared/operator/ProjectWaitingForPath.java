package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.sql.statement.selectstatement.UnarySelectStatement;

public class ProjectWaitingForPath implements Operator {

  private final UnarySelectStatement incompleteStatement;

  public ProjectWaitingForPath(UnarySelectStatement statement) {
    this.incompleteStatement = statement;
  }

  public UnarySelectStatement getIncompleteStatement() {
    return incompleteStatement;
  }

  @Override
  public OperatorType getType() {
    return OperatorType.ProjectWaitingForPath;
  }

  @Override
  public Operator copy() {
    return new ProjectWaitingForPath(incompleteStatement);
  }

  @Override
  public String getInfo() {
    return "";
  }
}
