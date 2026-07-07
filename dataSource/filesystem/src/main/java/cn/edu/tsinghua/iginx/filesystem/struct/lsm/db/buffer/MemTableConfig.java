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
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.buffer;

import cn.edu.tsinghua.iginx.filesystem.common.AbstractConfig;
import com.google.common.collect.Range;
import com.typesafe.config.ConfigMemorySize;
import com.typesafe.config.Optional;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.FieldNameConstants;
import org.apache.arrow.vector.BaseValueVector;

@Data
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class MemTableConfig extends AbstractConfig {

  @Optional ConfigMemorySize capacity = ConfigMemorySize.ofBytes(512 * 1024 * 1024);

  @Optional int chunkValues = BaseValueVector.INITIAL_VALUE_ALLOCATION;

  @Optional Duration timeout = Duration.ZERO;

  @Optional boolean enableAlignInsert = true;

  @Override
  public List<ValidationProblem> validate() {
    List<ValidationProblem> problems = new ArrayList<>();
    validateInRange(problems, Fields.chunkValues, Range.greaterThan(0), chunkValues);
    return problems;
  }
}
