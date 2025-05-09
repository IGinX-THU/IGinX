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
package cn.edu.tsinghua.iginx.filesystem.struct.tree;

import cn.edu.tsinghua.iginx.filesystem.common.AbstractConfig;
import com.typesafe.config.*;
import com.typesafe.config.Optional;
import java.util.*;
import lombok.*;
import lombok.experimental.FieldNameConstants;

@Data
@With
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
@AllArgsConstructor
@NoArgsConstructor
public class FileTreeConfig extends AbstractConfig {

  @Optional String dot = "\\";

  @Optional String prefix = null;

  @Optional int boundaryLevel = 0;

  @Optional Map<String, Config> formats = Collections.emptyMap();

  @Override
  public List<ValidationProblem> validate() {
    List<ValidationProblem> problems = new ArrayList<>();
    if (validateNotNull(problems, Fields.dot, dot)) {
      if (dot.contains(".")) {
        problems.add(new InvalidFieldValidationProblem(Fields.dot, "dot cannot contain '.'"));
      }
    }
    if(boundaryLevel != 0 && boundaryLevel != 1){
      problems.add(new InvalidFieldValidationProblem(Fields.boundaryLevel, "boundaryLevel must be 0 or 1"));
    }
    return problems;
  }

  @SuppressWarnings("unchecked")
  public static FileTreeConfig of(Config config) {
    Config withoutFormats = config.withoutPath(Fields.formats);
    FileTreeConfig fileTreeConfig = of(withoutFormats, FileTreeConfig.class);

    if (config.hasPath(Fields.formats)) {
      ConfigValue value = config.getValue(Fields.formats);
      if (value.valueType() == ConfigValueType.OBJECT) {
        Map<String, Object> formatsRawConfig = (Map<String, Object>) value.unwrapped();
        Map<String, Config> formats = new HashMap<>();
        for (Map.Entry<String, Object> entry : formatsRawConfig.entrySet()) {
          if (entry.getValue() instanceof Map) {
            formats.put(
                entry.getKey(), ConfigFactory.parseMap((Map<String, Object>) entry.getValue()));
          }
        }
        fileTreeConfig.setFormats(formats);
      }
    }

    return fileTreeConfig;
  }
}
