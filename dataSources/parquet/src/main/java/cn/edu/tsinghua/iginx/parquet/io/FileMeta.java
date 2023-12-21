package cn.edu.tsinghua.iginx.parquet.io;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface FileMeta {

  /**
   * get all fields' name in file
   *
   * @return all names of fields in file. null if not existed
   */
  @Nullable
  Set<String> fields();

  /**
   * get type of specified field
   *
   * @param field field type
   * @return type of specified field. null if not existed
   */
  @Nullable
  DataType getType(@Nonnull String field);

  /**
   * get extra information of file
   *
   * @return extra information of file. null if not existed
   */
  @Nullable
  Map<String, String> extra();
}
