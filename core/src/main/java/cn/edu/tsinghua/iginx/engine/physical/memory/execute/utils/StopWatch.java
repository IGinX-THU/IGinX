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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils;

import java.util.Objects;
import java.util.function.LongConsumer;

public class StopWatch implements AutoCloseable {

  private final LongConsumer timeAccumulator;
  private final long startTimestamp;

  public StopWatch(LongConsumer timeAccumulator) {
    this.timeAccumulator = Objects.requireNonNull(timeAccumulator);
    this.startTimestamp = System.currentTimeMillis();
  }

  @Override
  public void close() {
    timeAccumulator.accept(System.currentTimeMillis() - startTimestamp);
  }
}
