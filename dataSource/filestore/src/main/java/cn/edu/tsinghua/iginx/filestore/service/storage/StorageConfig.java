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
package cn.edu.tsinghua.iginx.filestore.service.storage;

import cn.edu.tsinghua.iginx.filestore.common.AbstractConfig;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.LegacyParquet;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.Optional;
import lombok.*;
import lombok.experimental.FieldNameConstants;

import java.util.List;

@Data
@With
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class StorageConfig extends AbstractConfig {
  String root;
  @Optional
  String struct = LegacyParquet.NAME;
  @Optional
  Config config = ConfigFactory.empty();

  @Override
  public List<ValidationProblem> validate() {
    throw new UnsupportedOperationException("Not implemented");
  }
}
