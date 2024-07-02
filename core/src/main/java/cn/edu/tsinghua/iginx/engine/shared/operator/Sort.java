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
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class Sort extends AbstractUnaryOperator {

  private final List<String> sortByCols;

  private final SortType sortType;

  public Sort(Source source, List<String> sortByCols, SortType sortType) {
    super(OperatorType.Sort, source);
    if (sortByCols == null || sortByCols.isEmpty()) {
      throw new IllegalArgumentException("sortBy shouldn't be null");
    }
    if (sortType == null) {
      throw new IllegalArgumentException("sortType shouldn't be null");
    }
    this.sortByCols = sortByCols;
    this.sortType = sortType;
  }

  public List<String> getSortByCols() {
    return sortByCols;
  }

  public SortType getSortType() {
    return sortType;
  }

  @Override
  public Operator copy() {
    return new Sort(getSource().copy(), new ArrayList<>(sortByCols), sortType);
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new Sort(source, new ArrayList<>(sortByCols), sortType);
  }

  public enum SortType {
    ASC,
    DESC
  }

  @Override
  public String getInfo() {
    return "SortBy: " + String.join(",", sortByCols) + ", SortType: " + sortType;
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    Sort sort = (Sort) object;
    return sortByCols.equals(sort.sortByCols) && sortType == sort.sortType;
  }
}
