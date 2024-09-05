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
import cn.edu.tsinghua.iginx.filestore.struct.legacy.filesystem.LegacyFilesystem;
import cn.edu.tsinghua.iginx.filestore.struct.legacy.parquet.LegacyParquet;
import com.typesafe.config.Config;
import com.typesafe.config.Optional;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.FieldNameConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class FileStoreConfig extends AbstractConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileStoreConfig.class);

  public static final String DEFAULT_DATA_STRUCT = LegacyParquet.NAME;
  public static final String DEFAULT_DUMMY_STRUCT = LegacyFilesystem.NAME;

  boolean serve;

  @Optional ClientConfig client = new ClientConfig();

  @Optional StorageConfig data = null;

  @Optional StorageConfig dummy = null;

  @Override
  public List<ValidationProblem> validate() {
    List<ValidationProblem> problems = new ArrayList<>();
    if (serve) {
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
    FileStoreConfig result = of(config, FileStoreConfig.class);

    if (result.isServe()) {
      LOGGER.debug("storage of {} is local, ignore config for remote", config);
      result.setClient(null);
    } else {
      LOGGER.debug("storage of {} is remote, ignore config for local", config);
      result.setData(null);
      result.setDummy(null);
    }

    return result;
  }
}
