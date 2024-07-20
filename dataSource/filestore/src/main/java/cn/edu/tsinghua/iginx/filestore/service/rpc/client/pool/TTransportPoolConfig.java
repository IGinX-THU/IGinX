package cn.edu.tsinghua.iginx.filestore.service.rpc.client.pool;

import cn.edu.tsinghua.iginx.filestore.common.AbstractConfig;
import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.FieldNameConstants;
import org.apache.commons.pool2.impl.BaseObjectPoolConfig;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class TTransportPoolConfig extends AbstractConfig {

  @Optional
  Duration minEvictableIdleDuration = BaseObjectPoolConfig.DEFAULT_MIN_EVICTABLE_IDLE_DURATION;

  @Optional
  int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;

  @Override
  public List<ValidationProblem> validate() {
    List<ValidationProblem> problems = new ArrayList<>();
    validateNotNull(problems, Fields.minEvictableIdleDuration, minEvictableIdleDuration);
    validateNotNull(problems, Fields.maxTotal, maxTotal);
    return problems;
  }
}
