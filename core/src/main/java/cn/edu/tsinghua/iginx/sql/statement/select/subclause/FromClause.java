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
package cn.edu.tsinghua.iginx.sql.statement.select.subclause;

import cn.edu.tsinghua.iginx.sql.statement.frompart.FromPart;
import java.util.ArrayList;
import java.util.List;

public class FromClause {
  private List<FromPart> fromParts;
  private boolean hasJoinParts = false;

  public FromClause() {
    fromParts = new ArrayList<>();
  }

  public void setFromParts(List<FromPart> fromParts) {
    this.fromParts = fromParts;
  }

  public List<FromPart> getFromParts() {
    return fromParts;
  }

  public void setHasJoinParts(boolean hasJoinParts) {
    this.hasJoinParts = hasJoinParts;
  }

  public boolean hasJoinParts() {
    return hasJoinParts;
  }

  public void addFromPart(FromPart fromPart) {
    fromParts.add(fromPart);
  }
}
