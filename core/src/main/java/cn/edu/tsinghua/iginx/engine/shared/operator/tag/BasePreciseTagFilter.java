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
package cn.edu.tsinghua.iginx.engine.shared.operator.tag;

import java.util.Map;
import java.util.StringJoiner;

public class BasePreciseTagFilter implements TagFilter {

  private final Map<String, String> tags;

  public BasePreciseTagFilter(Map<String, String> tags) {
    this.tags = tags;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  @Override
  public TagFilterType getType() {
    return TagFilterType.BasePrecise;
  }

  @Override
  public TagFilter copy() {
    return new BasePreciseTagFilter(tags);
  }

  @Override
  public String toString() {
    StringJoiner joiner = new StringJoiner(" && ", "(", ")");
    tags.forEach((k, v) -> joiner.add(k + "=" + v));
    return joiner.toString();
  }
}
