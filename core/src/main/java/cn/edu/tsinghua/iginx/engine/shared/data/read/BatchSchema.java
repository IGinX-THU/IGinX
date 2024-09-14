package cn.edu.tsinghua.iginx.engine.shared.data.read;

import java.util.Objects;
import org.apache.arrow.vector.types.pojo.Schema;

public class BatchSchema {
  private final Schema schema;

  protected BatchSchema(Schema schema) {
    this.schema = Objects.requireNonNull(schema);
  }
}
