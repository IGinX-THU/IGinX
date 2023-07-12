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
package cn.edu.tsinghua.iginx.engine.shared;

import java.util.Objects;

public final class KeyRange {

  private final long beginKey;

  private final boolean includeBeginKey;

  private final long endKey;

  private final boolean includeEndKey;

  public KeyRange(long beginKey, long endKey) {
    this(beginKey, true, endKey, false);
  }

  public KeyRange(long beginKey, boolean includeBeginKey, long endKey, boolean includeEndKey) {
    this.beginKey = beginKey;
    this.includeBeginKey = includeBeginKey;
    this.endKey = endKey;
    this.includeEndKey = includeEndKey;
  }

  public long getBeginKey() {
    return beginKey;
  }

  public boolean isIncludeBeginKey() {
    return includeBeginKey;
  }

  public long getEndKey() {
    return endKey;
  }

  public boolean isIncludeEndKey() {
    return includeEndKey;
  }

  public KeyRange copy() {
    return new KeyRange(beginKey, includeBeginKey, endKey, includeEndKey);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    KeyRange range = (KeyRange) o;
    return beginKey == range.beginKey
        && includeBeginKey == range.includeBeginKey
        && endKey == range.endKey
        && includeEndKey == range.includeEndKey;
  }

  @Override
  public int hashCode() {
    return Objects.hash(beginKey, includeBeginKey, endKey, includeEndKey);
  }

  @Override
  public String toString() {
    return (isIncludeBeginKey() ? "[" : "(")
        + beginKey
        + ", "
        + endKey
        + (isIncludeEndKey() ? "]" : ")");
  }

  public long getActualBeginKey() {
    if (includeBeginKey) {
      return beginKey;
    }
    return beginKey + 1;
  }

  public long getActualEndKey() {
    if (includeEndKey) {
      return endKey;
    }
    return endKey - 1;
  }
}
