package cn.edu.tsinghua.iginx.engine.shared.function.udf.utils;

import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import java.util.ArrayList;
import java.util.List;

public class CheckUtils {

  public static <T> List<T> castList(Object obj, Class<T> clazz) {
    List<T> result = new ArrayList<T>();
    if (obj instanceof List<?>) {
      for (Object o : (List<?>) obj) {
        result.add(clazz.cast(o));
      }
      return result;
    }
    return null;
  }

  public static boolean isLegal(FunctionParams params) {
    List<String> paths = params.getPaths();
    return paths != null && !paths.isEmpty();
  }
}
