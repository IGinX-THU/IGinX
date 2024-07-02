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
package cn.edu.tsinghua.iginx.engine.logical.generator;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.Optimizer;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractGenerator implements LogicalGenerator {

  protected GeneratorType type;

  private final List<Optimizer> optimizerList = new ArrayList<>();
  private static final Map<GeneratorType, StatementType> typeMap = new HashMap<>();

  static {
    typeMap.put(GeneratorType.Query, StatementType.SELECT);
    typeMap.put(GeneratorType.Insert, StatementType.INSERT);
    typeMap.put(GeneratorType.Delete, StatementType.DELETE);
    typeMap.put(GeneratorType.ShowColumns, StatementType.SHOW_COLUMNS);
  }

  public void registerOptimizer(Optimizer optimizer) {
    if (optimizer != null) optimizerList.add(optimizer);
  }

  @Override
  public GeneratorType getType() {
    return type;
  }

  @Override
  public Operator generate(RequestContext ctx) {
    Statement statement = ctx.getStatement();
    if (statement == null) return null;
    if (statement.getType() != typeMap.get(type)) return null;
    Operator root = generateRoot(statement);
    if (root != null) {
      for (Optimizer optimizer : optimizerList) {
        root = optimizer.optimize(root);
      }
    }
    return root;
  }

  protected abstract Operator generateRoot(Statement statement);
}
