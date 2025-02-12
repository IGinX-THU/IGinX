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

import java.util.Map;

public class IginXRecord {

  private final long key;

  private final IginXHeader header;

  private final Map<String, Object> values;

  public IginXRecord(IginXHeader header, Map<String, Object> values) {
    this.key = 0L;
    this.header = header;
    this.values = values;
  }

  public IginXRecord(long key, IginXHeader header, Map<String, Object> values) {
    this.key = key;
    this.header = header;
    this.values = values;
  }

  public IginXHeader getHeader() {
    return header;
  }

  public Map<String, Object> getValues() {
    return values;
  }

  public Object getValue(String measurement) {
    return values.get(measurement);
  }

  public boolean hasTimestamp() {
    return header.hasTimestamp();
  }

  public long getKey() {
    return key;
  }

  @Override
  public String toString() {
    return "IginXRecord{" + "values=" + values + '}';
  }
}
