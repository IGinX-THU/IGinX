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
  JOB_IDLE(602),
  JOB_RUNNING(603),
  JOB_PARTIALLY_FAILING(604),
  JOB_PARTIALLY_FAILED(605),
  JOB_FAILING(606),
  JOB_FAILED(607),
  JOB_CLOSING(608),
  JOB_CLOSED(609);

  private int statusCode;

  StatusCode(int statusCode) {
    this.statusCode = statusCode;
  }

  public int getStatusCode() {
    return statusCode;
  }
}
