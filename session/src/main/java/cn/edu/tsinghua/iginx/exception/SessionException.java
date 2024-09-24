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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.exception;

import cn.edu.tsinghua.iginx.thrift.Status;

/**
 * For more detailed information, please refer to
 * shared/src/main/java/cn/edu/tsinghua/iginx/exception/package-info.java
 */
public class SessionException extends Exception {

  private static final long serialVersionUID = -2811585771984779297L;

  protected int errorCode;

  public SessionException(Status status) {
    super(status.message);
    errorCode = status.code;
  }

  public SessionException(String message) {
    super(message);
    errorCode = StatusCode.SESSION_ERROR.getStatusCode();
  }

  public SessionException(Throwable cause) {
    super(cause);
    errorCode = StatusCode.SESSION_ERROR.getStatusCode();
  }

  public SessionException(String message, Throwable cause) {
    super(message, cause);
    errorCode = StatusCode.SESSION_ERROR.getStatusCode();
  }
}
