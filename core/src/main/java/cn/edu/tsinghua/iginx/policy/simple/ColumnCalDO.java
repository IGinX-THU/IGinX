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
package cn.edu.tsinghua.iginx.policy.simple;

import lombok.Data;

@Data
public class ColumnCalDO implements Comparable<ColumnCalDO> {

  private String column;

  private Long recentKey = 0L;

  private Long firstKey = Long.MAX_VALUE;

  private Long lastKey = Long.MIN_VALUE;

  private Integer count = 0;

  private Long totalByte = 0L;

  public Double getValue() {
    double ret = 0.0;
    if (count > 1 && lastKey > firstKey) {
      ret = 1.0 * totalByte / count * (count - 1) / (lastKey - firstKey);
    }
    return ret;
  }

  @Override
  public int compareTo(ColumnCalDO columnCalDO) {
    if (getValue() < columnCalDO.getValue()) {
      return -1;
    } else if (getValue() > columnCalDO.getValue()) {
      return 1;
    }
    return 0;
  }

  public void merge(Long recentKey, Long firstKey, Long lastKey, Integer count, Long totalByte) {
    this.recentKey = recentKey;
    this.firstKey = Math.min(firstKey, this.firstKey);
    this.lastKey = Math.max(lastKey, this.lastKey);
    this.count += count;
    this.totalByte += totalByte;
  }
}
