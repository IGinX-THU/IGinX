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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar.predicate.compare;

import java.util.Set;
import org.apache.arrow.memory.util.ArrowBufPointer;

public class NotInSet extends InSet {

  public static final String NAME_PREFIX = "not_in";

  public NotInSet(Set<Object> values) {
    super(NAME_PREFIX, values);
  }

  @Override
  protected boolean evaluate(boolean input) {
    return !super.evaluate(input);
  }

  @Override
  protected boolean evaluate(int input) {
    return !super.evaluate(input);
  }

  @Override
  protected boolean evaluate(long input) {
    return !super.evaluate(input);
  }

  @Override
  protected boolean evaluate(float input) {
    return !super.evaluate(input);
  }

  @Override
  protected boolean evaluate(double input) {
    return !super.evaluate(input);
  }

  @Override
  protected boolean evaluate(ArrowBufPointer input) {
    return !super.evaluate(input);
  }
}
