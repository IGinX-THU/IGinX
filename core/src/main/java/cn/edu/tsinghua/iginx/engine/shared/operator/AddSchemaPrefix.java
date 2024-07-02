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
package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class AddSchemaPrefix extends AbstractUnaryOperator {

  private final String schemaPrefix; // 可以为 null

  public AddSchemaPrefix(Source source, String schemaPrefix) {
    super(OperatorType.AddSchemaPrefix, source);
    this.schemaPrefix = schemaPrefix;
  }

  @Override
  public Operator copy() {
    return new AddSchemaPrefix(getSource().copy(), schemaPrefix);
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new AddSchemaPrefix(source, schemaPrefix);
  }

  @Override
  public String getInfo() {
    return "SchemaPrefix: " + schemaPrefix;
  }

  public String getSchemaPrefix() {
    return schemaPrefix;
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    AddSchemaPrefix that = (AddSchemaPrefix) object;
    return schemaPrefix.equals(that.schemaPrefix);
  }
}
