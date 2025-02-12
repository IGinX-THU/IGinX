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
package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class Insert extends AbstractUnaryOperator {

  private final DataView data;

  public Insert(FragmentSource source, DataView data) {
    super(OperatorType.Insert, source);
    if (data == null) {
      throw new IllegalArgumentException("raw data shouldn't be null");
    }
    this.data = data;
  }

  public DataView getData() {
    return data;
  }

  @Override
  public Operator copy() {
    // data should not be copied in memory.
    return new Insert((FragmentSource) getSource().copy(), data);
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new Insert((FragmentSource) source, data);
  }

  @Override
  public String getInfo() {
    return "";
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }

    Insert that = (Insert) object;
    return data.equals(that.data);
  }
}
