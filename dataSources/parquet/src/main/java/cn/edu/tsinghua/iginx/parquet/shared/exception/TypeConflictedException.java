/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.tsinghua.iginx.parquet.shared.exception;

public class TypeConflictedException extends SchemaException {
  private final String field;
  private final String type;
  private final String oldType;

  public TypeConflictedException(String field, String type, String oldType) {
    super(String.format("can't insert %s value into %s column at %s", type, oldType, field));
    this.field = field;
    this.type = type;
    this.oldType = oldType;
  }

  public String getField() {
    return field;
  }

  public String getType() {
    return type;
  }

  public String getOldType() {
    return oldType;
  }
}
