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
package cn.edu.tsinghua.iginx.metadata.entity;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.KEY_MAX_VAL;
import static cn.edu.tsinghua.iginx.constant.GlobalConstant.KEY_MIN_VAL;

import java.util.Objects;

public final class KeyInterval {

  private long startKey;

  private long endKey;

  public KeyInterval(long startKey, long endKey) {
    this.startKey = startKey;
    this.endKey = endKey;
  }

  public long getStartKey() {
    return startKey;
  }

  public void setStartKey(long startKey) {
    this.startKey = startKey;
  }

  public long getSpan() {
    return endKey - startKey;
  }

  public long getEndKey() {
    return endKey;
  }

  public void setEndKey(long endKey) {
    this.endKey = endKey;
  }

  @Override
  public String toString() {
    return "" + startKey;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    KeyInterval that = (KeyInterval) o;
    return startKey == that.startKey && endKey == that.endKey;
  }

  @Override
  public int hashCode() {
    return Objects.hash(startKey, endKey);
  }

  // 这里以及下面两个函数传入的都是闭区间
  public boolean isIntersect(KeyInterval keyInterval) {
    return (keyInterval.startKey < endKey) && (keyInterval.endKey >= startKey);
  }

  public boolean isBefore(KeyInterval keyInterval) {
    return endKey <= keyInterval.startKey;
  }

  public boolean isAfter(KeyInterval keyInterval) {
    return startKey > keyInterval.endKey;
  }

  public KeyInterval getIntersectWithLCRO(KeyInterval keyInterval) {
    long start = Math.max(keyInterval.startKey, startKey);
    long end = Math.min(keyInterval.endKey, endKey);
    return new KeyInterval(start, end);
  }

  public static KeyInterval getDefaultKeyInterval() {
    return new KeyInterval(KEY_MIN_VAL, KEY_MAX_VAL);
  }
}
