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

  @Optional Map<String, Integer> boundary = Collections.singletonMap("level", 0);

  @Optional Map<String, Config> formats = Collections.emptyMap();

  @Override
  public List<ValidationProblem> validate() {
    List<ValidationProblem> problems = new ArrayList<>();
    if (validateNotNull(problems, Fields.dot, dot)) {
      if (dot.contains(".")) {
        problems.add(new InvalidFieldValidationProblem(Fields.dot, "dot cannot contain '.'"));
      }
    }
    if (boundary.get("level") != 0 && boundary.get("level") != 1) {
      problems.add(
          new InvalidFieldValidationProblem(Fields.boundary, "boundaryLevel must be 0 or 1"));
    }
    return problems;
  }

  @SuppressWarnings("unchecked")
  public static FileTreeConfig of(Config config) {
    Config withoutFormats = config.withoutPath(Fields.formats);
    Config withoutBoundary = withoutFormats.withoutPath(Fields.boundary);
    FileTreeConfig fileTreeConfig = of(withoutBoundary, FileTreeConfig.class);

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
    if (config.hasPath(Fields.boundary)) {
      ConfigValue value = config.getValue(Fields.boundary);
      if (value.valueType() == ConfigValueType.OBJECT) {
        Map<String, Object> boundaryRawConfig = (Map<String, Object>) value.unwrapped();
        Object levelObj = boundaryRawConfig.get("level");
        int level = 0;
        if (levelObj instanceof Number) {
          level = ((Number) levelObj).intValue();
        } else if (levelObj instanceof String) {
          level = Integer.parseInt((String) levelObj);
        }
        fileTreeConfig.setBoundary(Collections.singletonMap("level", level));
      }
    }

    return fileTreeConfig;
  }
}
