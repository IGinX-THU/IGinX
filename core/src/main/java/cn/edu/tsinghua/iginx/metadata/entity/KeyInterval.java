/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.metadata.entity;

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
    return new KeyInterval(Long.MIN_VALUE, Long.MAX_VALUE);
  }
}
