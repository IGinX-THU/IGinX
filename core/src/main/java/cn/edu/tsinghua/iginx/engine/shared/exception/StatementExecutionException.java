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
package cn.edu.tsinghua.iginx.engine.shared.exception;

import cn.edu.tsinghua.iginx.exception.IginxException;
import cn.edu.tsinghua.iginx.exception.StatusCode;
import cn.edu.tsinghua.iginx.thrift.Status;

public class StatementExecutionException extends IginxException {

  private static final long serialVersionUID = -7769482614133326007L;

  public StatementExecutionException(Status status) {
    super(status.message, status.code);
  }

  public StatementExecutionException(String message) {
    super(message, StatusCode.STATEMENT_EXECUTION_ERROR.getStatusCode());
  }

  public StatementExecutionException(Throwable cause) {
    super(cause, StatusCode.STATEMENT_EXECUTION_ERROR.getStatusCode());
  }

  public StatementExecutionException(String message, Throwable cause) {
    super(message, cause, StatusCode.STATEMENT_EXECUTION_ERROR.getStatusCode());
  }
}
