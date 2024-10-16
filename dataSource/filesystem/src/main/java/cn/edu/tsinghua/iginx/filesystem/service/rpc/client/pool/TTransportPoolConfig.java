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
package cn.edu.tsinghua.iginx.filesystem.service.rpc.client.pool;

import cn.edu.tsinghua.iginx.filesystem.common.AbstractConfig;
import com.typesafe.config.Optional;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.FieldNameConstants;
import org.apache.commons.pool2.impl.BaseObjectPoolConfig;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

@Data
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class TTransportPoolConfig extends AbstractConfig {

  @Optional
  Duration minEvictableIdleDuration = BaseObjectPoolConfig.DEFAULT_MIN_EVICTABLE_IDLE_DURATION;

  @Optional int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;

  @Override
  public List<ValidationProblem> validate() {
    List<ValidationProblem> problems = new ArrayList<>();
    validateNotNull(problems, Fields.minEvictableIdleDuration, minEvictableIdleDuration);
    validateNotNull(problems, Fields.maxTotal, maxTotal);
    return problems;
  }
}
