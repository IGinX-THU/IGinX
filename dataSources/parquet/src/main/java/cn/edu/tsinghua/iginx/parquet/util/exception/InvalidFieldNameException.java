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

package cn.edu.tsinghua.iginx.parquet.util.exception;

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
