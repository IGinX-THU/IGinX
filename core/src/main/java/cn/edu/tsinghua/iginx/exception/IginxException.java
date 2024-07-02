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

/**
 * For more detailed information, please refer to
 * shared/src/main/java/cn/edu/tsinghua/iginx/exception/package-info.java
 */
public class IginxException extends Exception {

  private static final long serialVersionUID = -1355829042896588219L;
  protected int errorCode;

  public IginxException(String message, int errorCode) {
    super(message);
    this.errorCode = errorCode;
  }

  public IginxException(String message, Throwable cause, int errorCode) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  public IginxException(Throwable cause, int errorCode) {
    super(cause);
    this.errorCode = errorCode;
  }

  public int getErrorCode() {
    return errorCode;
  }
}
