package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.EmptySource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.sql.statement.select.UnarySelectStatement;

public class ProjectWaitingForPath extends AbstractUnaryOperator {

  private final UnarySelectStatement incompleteStatement;

  public ProjectWaitingForPath(UnarySelectStatement statement) {
    super(OperatorType.ProjectWaitingForPath, EmptySource.EMPTY_SOURCE);
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
  public UnaryOperator copyWithSource(Source source) {
    return new ProjectWaitingForPath(incompleteStatement);
  }

  @Override
  public String getInfo() {
    return "";
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }

    ProjectWaitingForPath that = (ProjectWaitingForPath) object;
    return incompleteStatement.equals(that.incompleteStatement);
  }
}
