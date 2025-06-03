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
package cn.edu.tsinghua.iginx.filesystem.format.csv;

import cn.edu.tsinghua.iginx.filesystem.common.AbstractConfig;
import com.typesafe.config.Config;
import com.typesafe.config.Optional;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.FieldNameConstants;

@Data
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class CsvReaderConfig extends AbstractConfig {

  @Optional boolean parseTypeFromHeader = true;
  @Optional boolean inferSchema = false;
  @Optional String delimiter = null;
  @Optional int sampleSize = 1;
  @Optional String dateFormat = "yyyy-MM-dd";

  @Override
  public List<ValidationProblem> validate() {
    List<ValidationProblem> problems = new ArrayList<>();
    if (sampleSize <= 0) {
      problems.add(new ValidationProblem("sampleSize", "must be greater than 0"));
    }
    return problems;
  }

  public static CsvReaderConfig of(Config config) {
    return of(config, CsvReaderConfig.class);
  }
}
