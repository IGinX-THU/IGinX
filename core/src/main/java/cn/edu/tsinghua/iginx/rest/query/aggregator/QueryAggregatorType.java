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
package cn.edu.tsinghua.iginx.rest.query.aggregator;

public enum QueryAggregatorType {
  MAX("max"),
  MIN("min"),
  SUM("sum"),
  COUNT("count"),
  AVG("avg"),
  FIRST("first"),
  LAST("last"),
  DEV("dev"),
  DIFF("diff"),
  DIV("div"),
  FILTER("filter"),
  SAVE_AS("save_as"),
  RATE("rate"),
  SAMPLER("sampler"),
  PERCENTILE("percentile"),
  SHOW_COLUMNS("show_columns"),
  NONE("");
  private final String type;

  QueryAggregatorType(String type) {
    this.type = type;
  }

  public String getType() {
    return type;
  }
}
