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
package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.sql.statement.select.SelectStatement;

public class InsertFromSelectStatement extends DataStatement {

  private final long keyOffset;

  private final SelectStatement subSelectStatement;

  private final InsertStatement subInsertStatement;

  public InsertFromSelectStatement(
      long keyOffset, SelectStatement subSelectStatement, InsertStatement subInsertStatement) {
    this.statementType = StatementType.INSERT_FROM_SELECT;
    this.keyOffset = keyOffset;
    this.subSelectStatement = subSelectStatement;
    this.subInsertStatement = subInsertStatement;
  }

  public long getKeyOffset() {
    return keyOffset;
  }

  public SelectStatement getSubSelectStatement() {
    return subSelectStatement;
  }

  public InsertStatement getSubInsertStatement() {
    return subInsertStatement;
  }
}
