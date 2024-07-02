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
