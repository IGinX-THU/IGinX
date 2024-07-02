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
package cn.edu.tsinghua.iginx.session_v2.query;

import java.util.Collections;
import java.util.List;

public class IginXTable {

  public static final IginXTable EMPTY_TABLE =
      new IginXTable(IginXHeader.EMPTY_HEADER, Collections.emptyList());

  private final IginXHeader header;

  private final List<IginXRecord> records;

  public IginXTable(IginXHeader header, List<IginXRecord> records) {
    this.header = header;
    this.records = records;
  }

  public IginXHeader getHeader() {
    return header;
  }

  public List<IginXRecord> getRecords() {
    return records;
  }

  @Override
  public String toString() {
    return "IginXTable{" + "header=" + header + ", records=" + records + '}';
  }
}
