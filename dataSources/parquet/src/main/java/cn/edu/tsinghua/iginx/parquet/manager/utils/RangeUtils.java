package cn.edu.tsinghua.iginx.parquet.manager.utils;

import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;

public class RangeUtils {
  public static KeyInterval toKeyInterval(Range<Long> span) {
    if (span.isEmpty()) {
      return new KeyInterval(0, 0);
    }

    long begin = 0, end = Long.MAX_VALUE;
    if (span.hasLowerBound()) {
      begin = span.lowerEndpoint();
      if (span.lowerBoundType() == BoundType.OPEN) {
        begin = Math.max(begin, begin + 1);
      }
    }
    if (span.hasUpperBound()) {
      end = span.upperEndpoint();
      if (span.upperBoundType() == BoundType.CLOSED) {
        end = Math.max(end, end + 1);
      }
    }
    return new KeyInterval(begin, end);
  }
}
