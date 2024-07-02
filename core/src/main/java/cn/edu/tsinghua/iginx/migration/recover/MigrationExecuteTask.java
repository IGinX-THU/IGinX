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
package cn.edu.tsinghua.iginx.migration.recover;

import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import lombok.Data;

@Data
public class MigrationExecuteTask {

  public static final String SEPARATOR = "-";
  public static final long RESHARD_MIGRATION_COST = 10;

  private FragmentMeta fragmentMeta;
  private String masterStorageUnitId;
  private Long sourceStorageId;
  private Long targetStorageId;
  private MigrationExecuteType migrationExecuteType;

  public MigrationExecuteTask(
      FragmentMeta fragmentMeta,
      String masterStorageUnitId,
      Long sourceStorageId,
      Long targetStorageId,
      MigrationExecuteType migrationExecuteType) {
    this.fragmentMeta = fragmentMeta;
    this.masterStorageUnitId = masterStorageUnitId;
    this.sourceStorageId = sourceStorageId;
    this.targetStorageId = targetStorageId;
    this.migrationExecuteType = migrationExecuteType;
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
        + masterStorageUnitId
        + SEPARATOR
        + sourceStorageId
        + SEPARATOR
        + targetStorageId
        + SEPARATOR
        + migrationExecuteType;
  }

  public static MigrationExecuteTask fromString(String input) {
    String[] tuples = input.split(SEPARATOR);
    return new MigrationExecuteTask(
        new FragmentMeta(
            tuples[2], tuples[3], Long.parseLong(tuples[0]), Long.parseLong(tuples[1]), tuples[4]),
        tuples[4],
        Long.parseLong(tuples[5]),
        Long.parseLong(tuples[6]),
        MigrationExecuteType.valueOf(tuples[7]));
  }
}
