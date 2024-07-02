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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;

public abstract class BinaryLazyStream implements RowStream {

  protected final RowStream streamA;

  protected final RowStream streamB;

  protected RequestContext context;

  public BinaryLazyStream(RowStream streamA, RowStream streamB) {
    this.streamA = streamA;
    this.streamB = streamB;
  }

  @Override
  public void close() throws PhysicalException {
    PhysicalException pe = null;
    try {
      streamA.close();
    } catch (PhysicalException e) {
      pe = e;
    }
    try {
      streamB.close();
    } catch (PhysicalException e) {
      pe = e;
    }
    if (pe != null) {
      throw pe;
    }
  }

  @Override
  public void setContext(RequestContext context) {
    this.context = context;
  }

  @Override
  public RequestContext getContext() {
    return context;
  }
}
