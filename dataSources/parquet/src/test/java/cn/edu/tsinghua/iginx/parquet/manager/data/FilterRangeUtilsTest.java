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
package cn.edu.tsinghua.iginx.parquet.manager.data;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.util.Arrays;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterRangeUtilsTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(FilterRangeUtilsTest.class);

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
                        new KeyFilter(Op.L, 1629965732001000000L)))));

    LOGGER.info("create filter: {}", filter);

    RangeSet<Long> expectedResult =
        ImmutableRangeSet.of(Range.closedOpen(1629965727000000000L, 1629965732001000000L));
    RangeSet<Long> actualResult = FilterRangeUtils.rangeSetOf(filter);

    assertEquals(expectedResult, actualResult);
  }
}
