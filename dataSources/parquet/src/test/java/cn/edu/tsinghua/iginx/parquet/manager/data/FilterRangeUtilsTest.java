/*
 * Copyright 2024 IGinX of Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
