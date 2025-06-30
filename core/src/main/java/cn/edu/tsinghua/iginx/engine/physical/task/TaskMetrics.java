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
package cn.edu.tsinghua.iginx.engine.physical.task;

import java.time.Duration;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class TaskMetrics {

  public static final TaskMetrics NO_OP =
      new TaskMetrics() {
        @Override
        public void accumulateAffectRows(long number) {}

        @Override
        public void accumulateCpuTime(long nanos) {}

        @Override
        public long affectRows() {
          return 0;
        }

        @Override
        public Duration cpuTime() {
          return Duration.ZERO;
        }
      };
  private final LongAdder affectRows = new LongAdder();
  private final LongAdder span = new LongAdder();

  public void accumulateAffectRows(long number) {
    affectRows.add(number);
  }

  public void accumulateCpuTime(long nanos) {
    this.span.add(nanos);
  }

  public long affectRows() {
    return affectRows.sum();
  }

  public Duration cpuTime() {
    return Duration.ofNanos(span.sum());
  }
}
