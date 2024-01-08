/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.tsinghua.iginx.parquet.entity;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

public class SequenceGenerator implements LongSupplier {

  private static final long DELTA = 1L;

  private final AtomicLong current = new AtomicLong();;

  @Override
  public long getAsLong() {
    return current.getAndAdd(DELTA);
  }

  public void reset(long last) {
    current.set(last + DELTA);
  }
}
