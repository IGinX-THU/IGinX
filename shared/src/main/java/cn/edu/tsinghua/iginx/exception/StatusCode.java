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
package cn.edu.tsinghua.iginx.exception;

public enum StatusCode {
  WRONG_USERNAME_OR_PASSWORD(100),
  ACCESS_DENY(101),

  SUCCESS_STATUS(200),
  PARTIAL_SUCCESS(204),

  REDIRECT(300),

  SESSION_ERROR(400),
  STATEMENT_EXECUTION_ERROR(401),
  STATEMENT_PARSE_ERROR(402),

  SYSTEM_ERROR(500),
  SERVICE_UNAVAILABLE(503),

  JOB_FINISHED(600),
  JOB_CREATED(601),
  JOB_RUNNING(602),
  JOB_FAILING(603),
  JOB_FAILED(604),
  JOB_CLOSING(605),
  JOB_CLOSED(606);

  private int statusCode;

  StatusCode(int statusCode) {
    this.statusCode = statusCode;
  }

  public int getStatusCode() {
    return statusCode;
  }
}
