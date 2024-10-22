/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.sql.statement.frompart;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.ALL_PATH_SUFFIX;
import static cn.edu.tsinghua.iginx.sql.SQLConstant.DOT;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;
import cn.edu.tsinghua.iginx.sql.statement.select.CommonTableExpression;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
  public List<Pair<String, String>> getAliasList() {
    return Collections.singletonList(
        new Pair<>(cte.getName() + ALL_PATH_SUFFIX, alias + ALL_PATH_SUFFIX));
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
