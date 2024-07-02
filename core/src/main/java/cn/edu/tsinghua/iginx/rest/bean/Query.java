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
package cn.edu.tsinghua.iginx.rest.bean;

import cn.edu.tsinghua.iginx.thrift.TimePrecision;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;

@Data
public class Query {
  private Long startAbsolute;
  private Long endAbsolute;
  private Long cacheTime;
  private String timeZone;
  private TimePrecision timePrecision;
  private List<QueryMetric> queryMetrics = new ArrayList<>();

  public void addQueryMetrics(QueryMetric queryMetric) {
    this.queryMetrics.add(queryMetric);
  }

  public void addFirstAggregator() {
    for (QueryMetric metric : queryMetrics) {
      metric.addFirstAggregator();
    }
  }

  public void setNullNewAnno() {
    for (QueryMetric metric : queryMetrics) {
      metric.setNewAnnotationLimit(new AnnotationLimit());
    }
  }
}
