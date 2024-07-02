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
package cn.edu.tsinghua.iginx.integration.expansion.iotdb;

import static cn.edu.tsinghua.iginx.thrift.StorageEngineType.iotdb12;

import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDB12CapacityExpansionIT extends BaseCapacityExpansionIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB12CapacityExpansionIT.class);

  public IoTDB12CapacityExpansionIT() {
    super(
        iotdb12,
        "username:root, password:root, sessionPoolSize:20",
        new IoTDB12HistoryDataGenerator());
    wrongExtraParams.add("username:root, password:wrong, sessionPoolSize:20");
    wrongExtraParams.add("username:wrong, password:root, sessionPoolSize:20");
  }
}
