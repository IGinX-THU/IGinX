package cn.edu.tsinghua.iginx.filestore.service;

import cn.edu.tsinghua.iginx.filestore.common.AbstractConfig;
import cn.edu.tsinghua.iginx.filestore.service.rpc.client.ClientConfig;
import cn.edu.tsinghua.iginx.filestore.service.storage.StorageConfig;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.FieldNameConstants;

import java.util.ArrayList;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class FileStoreConfig extends AbstractConfig {

  boolean server;

  @Optional
  ClientConfig client;

  @Optional
  StorageConfig data;

  @Optional
  StorageConfig dummy;

  @Override
  public List<ValidationProblem> validate() {
    List<ValidationProblem> problems = new ArrayList<>();
    if (server) {
      if (data == null && dummy == null) {
        problems.add(new ValidationProblem(null, "either data or dummy storage must be configured if server is enable"));
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
}
