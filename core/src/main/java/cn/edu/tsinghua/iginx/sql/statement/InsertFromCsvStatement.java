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

import cn.edu.tsinghua.iginx.engine.shared.file.read.ImportFile;

public class InsertFromCsvStatement extends DataStatement {

  private final ImportFile importFile;

  private final InsertStatement subInsertStatement;

  private final long keyBase;

  private final String keyCol;

  public InsertFromCsvStatement(
      ImportFile importFile, InsertStatement subInsertStatement, long keyBase, String keyCol) {
    this.statementType = StatementType.INSERT_FROM_CSV;
    this.importFile = importFile;
    this.subInsertStatement = subInsertStatement;
    this.keyBase = keyBase;
    this.keyCol = keyCol;
  }

  public ImportFile getImportFile() {
    return importFile;
  }

  public InsertStatement getSubInsertStatement() {
    return subInsertStatement;
  }

  public long getKeyBase() {
    return keyBase;
  }

  public String getKeyCol() {
    return keyCol;
  }
}
