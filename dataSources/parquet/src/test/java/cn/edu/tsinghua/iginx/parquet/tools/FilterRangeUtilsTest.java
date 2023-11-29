package cn.edu.tsinghua.iginx.parquet.tools;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.parquet.entity.RangeSet;
import java.util.Arrays;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterRangeUtilsTest {

  private static final Logger logger = LoggerFactory.getLogger(FilterRangeUtilsTest.class);

  @Test
  public void SingleRange() {
    Filter filter =
        new AndFilter(
            Arrays.asList(
                new KeyFilter(Op.GE, 0),
                new KeyFilter(Op.L, Long.MAX_VALUE),
                new AndFilter(
                    Arrays.asList(
                        new KeyFilter(Op.GE, 1629965727000000000L),
                        new KeyFilter(Op.LE, 1629965732001000000L)))));

    logger.info("create filter: {}", filter);

    RangeSet<Long> expectedResult = new RangeSet<>(1629965727000000000L, 1629965732001000000L);
    RangeSet<Long> actualResult = FilterRangeUtils.rangeSetOf(filter);

    assertEquals(expectedResult, actualResult);
  }
}
