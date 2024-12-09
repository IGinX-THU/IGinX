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

import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;
import cn.edu.tsinghua.iginx.utils.Pair;
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

  @Override
  public String getOriginPrefix() {
    return path;
  }

  @Override
  public FromPartType getType() {
    return FromPartType.Path;
  }

  @Override
  public List<Pair<String, String>> getAliasList() {
    return Collections.singletonList(new Pair<>(path + ALL_PATH_SUFFIX, alias + ALL_PATH_SUFFIX));
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
