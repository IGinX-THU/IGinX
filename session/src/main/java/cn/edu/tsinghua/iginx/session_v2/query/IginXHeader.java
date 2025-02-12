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
package cn.edu.tsinghua.iginx.session_v2.query;

import java.util.Collections;
import java.util.List;

public class IginXHeader {

  public static final IginXHeader EMPTY_HEADER = new IginXHeader(Collections.emptyList());

  private final IginXColumn time;

  private final List<IginXColumn> columns;

  public IginXHeader(List<IginXColumn> columns) {
    this.time = null;
    this.columns = columns;
  }

  public IginXHeader(IginXColumn time, List<IginXColumn> columns) {
    this.time = time;
    this.columns = columns;
  }

  public List<IginXColumn> getColumns() {
    return columns;
  }

  public boolean hasTimestamp() {
    return this.time != null;
  }

  @Override
  public String toString() {
    return "IginXHeader{" + "time=" + time + ", columns=" + columns + '}';
  }
}
