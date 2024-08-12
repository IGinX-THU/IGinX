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
package cn.edu.tsinghua.iginx.filestore.service;

import cn.edu.tsinghua.iginx.filestore.common.AbstractConfig;
import cn.edu.tsinghua.iginx.filestore.service.rpc.client.ClientConfig;
import cn.edu.tsinghua.iginx.filestore.service.storage.StorageConfig;
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
public class FileStoreConfig extends AbstractConfig {

  boolean server;

  @Optional ClientConfig client = new ClientConfig();

  @Optional StorageConfig data = null;

  @Optional StorageConfig dummy = null;

  @Override
  public List<ValidationProblem> validate() {
    List<ValidationProblem> problems = new ArrayList<>();
    if (server) {
      if (data == null && dummy == null) {
        problems.add(
            new ValidationProblem(
                null, "either data or dummy storage must be configured if server is enable"));
      }
      if (data != null) {
        validateSubConfig(problems, Fields.data, data);
      }
      if (dummy != null) {
        validateSubConfig(problems, Fields.dummy, dummy);
      }
    } else {
      validateSubConfig(problems, Fields.client, client);
    }
    return problems;
  }

  public static FileStoreConfig of(Config config) {
    return of(config, FileStoreConfig.class);
  }
}
