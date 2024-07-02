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
package cn.edu.tsinghua.iginx.migration;

import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import lombok.Data;

@Data
public class MigrationTask {

  public static final String SEPARATOR = "-";
  public static final long RESHARD_MIGRATION_COST = 10;

  private FragmentMeta fragmentMeta;
  private long load;
  private long size;
  private Long sourceStorageId;
  private Long targetStorageId;
  private MigrationType migrationType;

  public MigrationTask(
      FragmentMeta fragmentMeta,
      long load,
      long size,
      Long sourceStorageId,
      Long targetStorageId,
      MigrationType migrationType) {
    this.fragmentMeta = fragmentMeta;
    this.load = load;
    this.size = size;
    this.sourceStorageId = sourceStorageId;
    this.targetStorageId = targetStorageId;
    this.migrationType = migrationType;
  }

  public double getPriorityScore() {
    switch (migrationType) {
      case WRITE:
        return load * 1.0 / RESHARD_MIGRATION_COST;
      case QUERY:
      default:
        return load * 1.0 / size;
    }
  }

  public long getMigrationSize() {
    switch (migrationType) {
      case WRITE:
        return RESHARD_MIGRATION_COST;
      case QUERY:
      default:
        return size;
    }
  }

  @Override
  public String toString() {
    return fragmentMeta.getKeyInterval().getStartKey()
        + SEPARATOR
        + fragmentMeta.getKeyInterval().getEndKey()
        + SEPARATOR
        + fragmentMeta.getColumnsInterval().getStartColumn()
        + SEPARATOR
        + fragmentMeta.getColumnsInterval().getEndColumn()
        + SEPARATOR
        + fragmentMeta.getMasterStorageUnitId()
        + SEPARATOR
        + load
        + SEPARATOR
        + size
        + SEPARATOR
        + sourceStorageId
        + SEPARATOR
        + targetStorageId
        + SEPARATOR
        + migrationType;
  }

  public static MigrationTask fromString(String input) {
    String[] tuples = input.split(SEPARATOR);
    return new MigrationTask(
        new FragmentMeta(
            tuples[2], tuples[3], Long.parseLong(tuples[0]), Long.parseLong(tuples[1]), tuples[4]),
        Long.parseLong(tuples[5]),
        Long.parseLong(tuples[6]),
        Long.parseLong(tuples[7]),
        Long.parseLong(tuples[8]),
        MigrationType.valueOf(tuples[9]));
  }
}
