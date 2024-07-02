package cn.edu.tsinghua.iginx.parquet.db.util.iterator;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nonnull;

public class SizeUtils {
  private SizeUtils() {}

  private static final Map<Class<?>, Function<Object, Long>> sizeMap = new HashMap<>();

  static {
    sizeMap.put(Boolean.class, (obj) -> 1L);
    sizeMap.put(Integer.class, (obj) -> 4L);
    sizeMap.put(Long.class, (obj) -> 8L);
    sizeMap.put(Float.class, (obj) -> 4L);
    sizeMap.put(Double.class, (obj) -> 8L);
    sizeMap.put(byte[].class, (obj) -> (long) ((byte[]) obj).length);
  }

  public static long sizeOf(@Nonnull Object obj) {
    Function<Object, Long> sizeGetter = sizeMap.get(obj.getClass());
    if (sizeGetter == null) {
      throw new UnsupportedOperationException(obj.getClass() + " is not supported");
    }
    return sizeGetter.apply(obj);
  }
}
