/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.storage.data.tsfile;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.tsfile.read.expression.IExpression;
import org.apache.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;

public class FilterUtils {

  private FilterUtils() {}

  @Nullable
  public static IExpression toTsfileExpression(RangeSet<Long> rangeSet) {
    List<Filter> ranges = new ArrayList<>();
    for (Range<Long> range : rangeSet.asRanges()) {
      Filter filter = toTsfileFilter(range);
      if (filter != null) {
        ranges.add(filter);
      }
    }
    Filter filter = toTsfileOrFilter(ranges);
    if (filter == null) {
      return null;
    }
    return new GlobalTimeExpression(filter);
  }

  @Nullable
  public static Filter toTsfileFilter(Range<Long> range) {
    Filter left;
    if (range.hasLowerBound()) {
      long lowerBound = range.lowerEndpoint();
      if (range.lowerBoundType() == BoundType.CLOSED) {
        left = TimeFilterApi.gtEq(lowerBound);
      } else {
        left = TimeFilterApi.gt(lowerBound);
      }
    } else {
      left = null;
    }
    Filter right;
    if (range.hasUpperBound()) {
      long upperBound = range.upperEndpoint();
      if (range.upperBoundType() == BoundType.CLOSED) {
        right = TimeFilterApi.ltEq(upperBound);
      } else {
        right = TimeFilterApi.lt(upperBound);
      }
    } else {
      right = null;
    }
    return FilterFactory.and(left, right);
  }

  @Nullable
  public static Filter toTsfileOrFilter(List<Filter> filters) {
    if (filters.isEmpty()) {
      return null;
    } else if (filters.size() == 1) {
      return filters.get(0);
    } else {
      return FilterFactory.or(filters);
    }
  }
}
