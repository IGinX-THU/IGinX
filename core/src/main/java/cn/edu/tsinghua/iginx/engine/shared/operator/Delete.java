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
        new ArrayList<>(keyRanges),
        new ArrayList<>(patterns),
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
}
