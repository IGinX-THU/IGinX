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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db;

import cn.edu.tsinghua.iginx.filesystem.common.AbstractConfig;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer.MemTableConfig;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.metadata.CatalogConfig;
import cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.StorageConfig;
import com.typesafe.config.Optional;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.FieldNameConstants;

@Data
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class DBConfig extends AbstractConfig {

  @Optional MemTableConfig memtable = new MemTableConfig();

  @Optional CatalogConfig catalog = new CatalogConfig();

  @Optional StorageConfig storage = new StorageConfig();

  @Optional boolean flushOnClose = true;

  @Override
  public List<ValidationProblem> validate() {
    List<ValidationProblem> problems = new ArrayList<>();
    validateSubConfig(problems, Fields.memtable, memtable);
    validateSubConfig(problems, Fields.catalog, catalog);
    validateSubConfig(problems, Fields.storage, storage);
    return problems;
  }
}
