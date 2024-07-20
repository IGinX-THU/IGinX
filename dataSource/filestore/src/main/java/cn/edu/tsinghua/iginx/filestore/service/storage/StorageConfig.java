package cn.edu.tsinghua.iginx.filestore.service.storage;

import cn.edu.tsinghua.iginx.filestore.common.AbstractConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.Optional;
import lombok.*;
import lombok.experimental.FieldNameConstants;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Data
@With
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class StorageConfig extends AbstractConfig {
  String root;
  @Optional
  String type;
  @Optional
  Config config = ConfigFactory.empty();

  @Override
  public List<ValidationProblem> validate() {
    throw new UnsupportedOperationException("Not implemented");
  }
}
