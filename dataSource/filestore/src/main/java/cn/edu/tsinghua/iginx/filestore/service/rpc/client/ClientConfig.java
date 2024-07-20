package cn.edu.tsinghua.iginx.filestore.service.rpc.client;

import cn.edu.tsinghua.iginx.filestore.common.AbstractConfig;
import cn.edu.tsinghua.iginx.filestore.service.rpc.client.pool.TTransportPoolConfig;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.FieldNameConstants;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class ClientConfig extends AbstractConfig {

  @Optional
  Duration socketTimeout = Duration.ZERO;
  @Optional
  Duration connectTimeout = Duration.ZERO;
  TTransportPoolConfig connectPool;

  @Override
  public List<ValidationProblem> validate() {
    List<ValidationProblem> problems = Collections.emptyList();
    validateNotNull(problems, Fields.socketTimeout, socketTimeout);
    validateNotNull(problems, Fields.connectTimeout, connectTimeout);
    validateSubConfig(problems, Fields.connectPool, connectPool);
    return problems;
  }
}
