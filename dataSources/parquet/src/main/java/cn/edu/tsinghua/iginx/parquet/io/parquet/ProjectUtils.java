package cn.edu.tsinghua.iginx.parquet.io.parquet;

import cn.edu.tsinghua.iginx.parquet.util.Constants;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import shaded.iginx.org.apache.parquet.schema.MessageType;
import shaded.iginx.org.apache.parquet.schema.Type;
import shaded.iginx.org.apache.parquet.schema.Types;

public class ProjectUtils {
  private ProjectUtils() {}

  @Nonnull
  static MessageType projectMessageType(@Nonnull MessageType schema, @Nullable Set<String> fields) {
    Set<String> schemaFields = new HashSet<>(Objects.requireNonNull(fields));
    schemaFields.add(Constants.KEY_FIELD_NAME);

    Types.MessageTypeBuilder builder = Types.buildMessage();
    for (String field : schemaFields) {
      if (schema.containsField(field)) {
        Type type = schema.getType(field);
        if (!type.isPrimitive()) {
          throw new IllegalArgumentException("not primitive type is not supported: " + field);
        }
        builder.addField(schema.getType(field));
      }
    }

    return builder.named(schema.getName());
  }
}
