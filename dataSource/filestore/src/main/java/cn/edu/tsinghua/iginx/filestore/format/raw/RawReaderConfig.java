package cn.edu.tsinghua.iginx.filestore.format.raw;

import cn.edu.tsinghua.iginx.filestore.common.AbstractConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigMemorySize;
import com.typesafe.config.Optional;
import java.util.Collections;
import java.util.List;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.FieldNameConstants;

@Data
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class RawReaderConfig extends AbstractConfig {

  @Optional ConfigMemorySize pageSize = ConfigMemorySize.ofBytes(4096);

  @Override
  public List<ValidationProblem> validate() {
    return Collections.emptyList();
  }

  public static RawReaderConfig of(Config config) {
    return of(config, RawReaderConfig.class);
  }
}
