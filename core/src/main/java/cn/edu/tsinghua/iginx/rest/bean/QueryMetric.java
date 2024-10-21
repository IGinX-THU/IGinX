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
package cn.edu.tsinghua.iginx.rest.bean;

import static cn.edu.tsinghua.iginx.rest.RestUtils.TOP_KEY;

import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregator;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorFirst;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Data;

@Data
public class QueryMetric {
  private String name;
  private String pathName;
  private String queryOriPath;
  private Long limit;
  private List<Map<String, List<String>>> tags = new ArrayList<>();
  private List<QueryAggregator> aggregators = new ArrayList<>();
  private Boolean annotation = false;
  private Boolean newAnnotation = false;
  private AnnotationLimit annotationLimit;
  private AnnotationLimit newAnnotationLimit;

  public void setQueryOriPath(String path) {
    queryOriPath = new String(path);
  }

  public void addTag(Map<String, List<String>> tags) {
    this.tags.add(tags);
  }

  public void addAggregator(QueryAggregator qa) {
    aggregators.add(qa);
  }

  public void addFirstAggregator() {
    QueryAggregator qa;
    qa = new QueryAggregatorFirst();
    qa.setDur(TOP_KEY);
    addAggregator(qa);
  }

  public void addCategory(String key) {
    if (annotationLimit == null) annotationLimit = new AnnotationLimit();
    annotationLimit.addTag(key);
  }
}
