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
