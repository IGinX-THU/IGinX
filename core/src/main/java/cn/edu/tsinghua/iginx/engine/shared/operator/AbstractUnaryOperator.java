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
import cn.edu.tsinghua.iginx.engine.shared.operator.visitor.OperatorVisitor;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;

public abstract class AbstractUnaryOperator extends AbstractOperator implements UnaryOperator {

  private Source source;

  public AbstractUnaryOperator(OperatorType type, Source source) {
    super(type);
    if (source == null) {
      throw new IllegalArgumentException("source shouldn't be null");
    }
    this.source = source;
  }

  public AbstractUnaryOperator(Source source) {
    this(OperatorType.Unary, source);
  }

  @Override
  public void accept(OperatorVisitor visitor) {
    visitor.enter();
    visitor.visit(this);

    if (visitor.needStop()) {
      return;
    }

    Source source = this.getSource();
    if (source.getType() == SourceType.Operator) {
      ((OperatorSource) source).getOperator().accept(visitor);
    }
    visitor.leave();
  }

  @Override
  public Source getSource() {
    return source;
  }

  @Override
  public void setSource(Source source) {
    this.source = source;
  }
}
