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

import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.sql.statement.ShowColumnsStatement;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
  public List<Pair<String, String>> getAliasList() {
    List<Pair<String, String>> aliasList = new ArrayList<>(2);
    aliasList.add(new Pair<>("Path", alias + ".Path"));
    aliasList.add(new Pair<>("Type", alias + ".Type"));
    return aliasList;
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
        : new ArrayList<>(Arrays.asList("Path", "Type"));
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
