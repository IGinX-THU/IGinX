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
package cn.edu.tsinghua.iginx.engine.physical.task;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;

public class TaskExecuteResult {

  private int affectRows;

  private RowStream rowStream;

  private PhysicalException exception;

  public TaskExecuteResult() {}

  public TaskExecuteResult(RowStream rowStream) {
    this(rowStream, null);
    if (rowStream instanceof Table) {
      Table table = (Table) rowStream;
      affectRows = table.getRowSize();
    }
  }

  public TaskExecuteResult(PhysicalException exception) {
    this(null, exception);
  }

  public TaskExecuteResult(RowStream rowStream, PhysicalException exception) {
    this.rowStream = rowStream;
    this.exception = exception;
  }

  public RowStream getRowStream() {
    RowStream rowStream = this.rowStream;
    this.rowStream = null;
    return rowStream;
  }

  public void setRowStream(RowStream rowStream) {
    this.rowStream = rowStream;
  }

  public PhysicalException getException() {
    return exception;
  }

  public void setException(PhysicalException exception) {
    this.exception = exception;
  }

  public int getAffectRows() {
    return affectRows;
  }
}
