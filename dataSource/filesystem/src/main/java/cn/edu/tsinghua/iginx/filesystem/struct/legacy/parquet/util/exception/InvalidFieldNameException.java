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
package cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.util.exception;

public class InvalidFieldNameException extends SchemaException {

  private final String fieldName;

  private final String reason;

  public InvalidFieldNameException(String fieldName, String reason) {
    super(String.format("invalid field name %s, because: ", fieldName, reason));
    this.fieldName = fieldName;
    this.reason = reason;
  }

  public String getFieldName() {
    return fieldName;
  }

  public String getReason() {
    return reason;
  }
}
