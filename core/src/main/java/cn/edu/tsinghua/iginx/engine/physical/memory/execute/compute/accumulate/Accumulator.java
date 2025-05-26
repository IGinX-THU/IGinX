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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.PhysicalFunction;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputingCloseable;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.exception.ComputeException;
import java.util.List;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

// TODO: 把 Accumulator 替代 State 成为有状态的迭代对象，同时支持 groupAccumulator
public interface Accumulator extends PhysicalFunction {

  State createState() throws ComputeException;

  void update(State state, VectorSchemaRoot input) throws ComputeException;

  FieldVector evaluate(List<State> states) throws ComputeException;

  interface State extends ComputingCloseable {}
}
