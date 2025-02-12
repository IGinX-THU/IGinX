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
package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.engine.shared.file.FileType;
import cn.edu.tsinghua.iginx.engine.shared.file.write.ExportFile;
import cn.edu.tsinghua.iginx.sql.statement.select.SelectStatement;

public class ExportFileFromSelectStatement extends DataStatement {

  private final SelectStatement selectStatement;

  private final ExportFile exportFile;

  public ExportFileFromSelectStatement(SelectStatement selectStatement, ExportFile exportFile) {
    if (exportFile.getType().equals(FileType.CSV)) {
      this.statementType = StatementType.EXPORT_CSV_FROM_SELECT;
    } else if (exportFile.getType().equals(FileType.BYTE_STREAM)) {
      this.statementType = StatementType.EXPORT_STREAM_FROM_SELECT;
    }
    this.selectStatement = selectStatement;
    this.exportFile = exportFile;
  }

  public SelectStatement getSelectStatement() {
    return selectStatement;
  }

  public ExportFile getExportFile() {
    return exportFile;
  }
}
