package cn.edu.tsinghua.iginx.filestore.common;

import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

import java.util.ArrayList;
import java.util.List;

public class Ranges {
  private Ranges() {
  }

  public static Range<Long> toClosedLongRange(Range<Long> range) {
    long lower = Long.MIN_VALUE;
    if(range.hasLowerBound()) {
      lower = range.lowerEndpoint();
      if(BoundType.OPEN.equals(range.lowerBoundType())) {
        lower++;
      }
    }
    long upper = Long.MAX_VALUE;
    if(range.hasUpperBound()) {
      upper = range.upperEndpoint();
      if(BoundType.OPEN.equals(range.upperBoundType())) {
        upper--;
      }
    }
    return Range.closed(lower, upper);
  }

  public static List<KeyRange> toKeyRanges(RangeSet<Long> rangeset){
    List<KeyRange> keyRanges = new ArrayList<>();
    for(Range<Long> range : rangeset.asRanges()) {
      Range<Long> closedRange = toClosedLongRange(range);
      KeyRange keyRange = new KeyRange(closedRange.lowerEndpoint(), true, closedRange.upperEndpoint(), true);
      keyRanges.add(keyRange);
    }
    return keyRanges;
  }
}
