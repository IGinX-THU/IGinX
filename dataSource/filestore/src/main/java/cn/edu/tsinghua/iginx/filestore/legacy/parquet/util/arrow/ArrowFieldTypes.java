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
package cn.edu.tsinghua.iginx.filestore.legacy.parquet.util.arrow;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.FieldType;

public class ArrowFieldTypes {

  public static FieldType of(
      boolean nullable,
      DataType dataType,
      @Nullable DictionaryEncoding dictionary,
      @Nullable Map<String, String> metadata) {
    Preconditions.checkNotNull(dataType, "dataType");

    return new FieldType(nullable, ArrowTypes.of(dataType), dictionary, metadata);
  }

  public static FieldType nonnull(DataType dataType, @Nullable Map<String, String> metadata) {
    return of(false, dataType, null, null);
  }

  public static FieldType nonnull(DataType dataType) {
    return nonnull(dataType, null);
  }
}
