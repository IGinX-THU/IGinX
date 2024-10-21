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
package cn.edu.tsinghua.iginx.session_v2.exception;

public class IginXException extends RuntimeException {

  private final String message;

  public IginXException(String message) {
    this.message = message;
  }

  public IginXException(Throwable cause) {
    super(cause);
    this.message = cause.getMessage();
  }

  public IginXException(String message, Throwable cause) {
    super(cause);
    this.message = message + cause.getMessage();
  }

  @Override
  public String getMessage() {
    return message;
  }
}
