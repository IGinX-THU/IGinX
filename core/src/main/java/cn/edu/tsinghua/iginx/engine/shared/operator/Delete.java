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
package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class Delete extends AbstractUnaryOperator {

  private final List<KeyRange> keyRanges;
  private final List<String> patterns;

  private final TagFilter tagFilter;

  public Delete(
      FragmentSource source, List<KeyRange> keyRanges, List<String> patterns, TagFilter tagFilter) {
    super(OperatorType.Delete, source);
    this.keyRanges = keyRanges;
    this.patterns = patterns;
    this.tagFilter = tagFilter;
  }

  public List<KeyRange> getKeyRanges() {
    return keyRanges;
  }

  public List<String> getPatterns() {
    return patterns;
  }

  public TagFilter getTagFilter() {
    return tagFilter;
  }

  @Override
  public Operator copy() {
    return new Delete(
        (FragmentSource) getSource().copy(),
        keyRanges == null ? null : new ArrayList<>(keyRanges),
        patterns == null ? null : new ArrayList<>(patterns),
        tagFilter == null ? null : tagFilter.copy());
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new Delete(
        (FragmentSource) source,
        new ArrayList<>(keyRanges),
        new ArrayList<>(patterns),
        tagFilter == null ? null : tagFilter.copy());
  }

  @Override
  public String getInfo() {
    StringBuilder builder = new StringBuilder();
    builder.append("Patterns: ");
    for (String pattern : patterns) {
      builder.append(pattern).append(",");
    }
    builder.deleteCharAt(builder.length() - 1);
    if (tagFilter != null) {
      builder.append(", TagFilter: ").append(tagFilter.toString());
    }
    return builder.toString();
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    if (!super.equals(object)) {
      return false;
    }
    Delete that = (Delete) object;
    return keyRanges.equals(that.keyRanges)
        && patterns.equals(that.patterns)
        && tagFilter.equals(that.tagFilter);
  }
}
