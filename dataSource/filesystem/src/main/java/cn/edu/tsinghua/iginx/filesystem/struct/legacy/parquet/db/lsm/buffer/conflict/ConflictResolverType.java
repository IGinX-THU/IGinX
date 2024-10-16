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
package cn.edu.tsinghua.iginx.filesystem.struct.legacy.parquet.db.lsm.buffer.conflict;

import java.util.Objects;
import java.util.function.Supplier;

public enum ConflictResolverType {
  TRY_LOCK(TryLockResolver::new),
  REC_TRY_LOCK(RecursiveTryLockResolver::new),
  MULTI_QUEUE(ThreadPoolResolver::new),
  NONE(NoneResolver::new);

  private final Supplier<ConflictResolver> factory;

  ConflictResolverType(Supplier<ConflictResolver> factory) {
    this.factory = Objects.requireNonNull(factory);
  }

  public ConflictResolver create() {
    return factory.get();
  }
}
