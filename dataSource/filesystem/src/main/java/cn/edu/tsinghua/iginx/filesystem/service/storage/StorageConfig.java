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
package cn.edu.tsinghua.iginx.filesystem.service.storage;

import cn.edu.tsinghua.iginx.filesystem.common.AbstractConfig;
import cn.edu.tsinghua.iginx.filesystem.struct.FileStructure;
import cn.edu.tsinghua.iginx.filesystem.struct.FileStructureManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.Optional;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import lombok.*;
import lombok.experimental.FieldNameConstants;

@Data
@With
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class StorageConfig extends AbstractConfig {
  String root;
  String struct;
  @Optional Config config = ConfigFactory.empty();

  @Override
  public List<ValidationProblem> validate() {
    List<ValidationProblem> problems = new ArrayList<>();
    validateNotBlanks(problems, Fields.root, root);
    validateNotNull(problems, Fields.struct, struct);
    validateNotNull(problems, Fields.config, config);
    FileStructure fileStructure = FileStructureManager.getInstance().getByName(struct);
    if (fileStructure == null) {
      problems.add(new ValidationProblem(Fields.struct, "Unknown file structure: " + struct));
    } else {
      try (Closeable shared = fileStructure.newShared(config)) {
        // TODO: check file structure, return some warnging
      } catch (IOException e) {
        problems.add(new ValidationProblem(Fields.config, "Invalid config: " + e));
      }
    }
    return problems;
  }
}
