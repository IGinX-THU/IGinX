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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class GroupByKey {

  private final List<Object> groupByValues; // the values of group by cols.

  private final List<Object> funcRet = new ArrayList<>(); // apply SetTransform Func result

  private final int hash;

  public GroupByKey(List<Object> groupByValues) {
    this.groupByValues = groupByValues;
    this.hash = groupByValues.hashCode();
  }

  public List<Object> getGroupByValues() {
    return groupByValues;
  }

  public int getHash() {
    return hash;
  }

  public List<Object> getFuncRet() {
    return funcRet;
  }

  public List<Object> getRowValues() {
    List<Object> ret = new ArrayList<>(groupByValues);
    ret.addAll(funcRet);
    return ret;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GroupByKey that = (GroupByKey) o;
    return hash == that.hash && Objects.equals(groupByValues, that.groupByValues);
  }

  @Override
  public int hashCode() {
    return hash;
  }
}
