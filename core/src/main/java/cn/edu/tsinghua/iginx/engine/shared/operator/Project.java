package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import java.util.ArrayList;
import java.util.List;

public class Project extends AbstractUnaryOperator {

  private final List<String> patterns;

  private final TagFilter tagFilter;

  private final boolean remainKey; // 是否保留以key结尾的field

  private boolean needSelectedPath;

  public Project(Source source, List<String> patterns, TagFilter tagFilter) {
    this(source, patterns, tagFilter, false, false);
  }

  public Project(
      Source source, List<String> patterns, TagFilter tagFilter, boolean needSelectedPath) {
    this(source, patterns, tagFilter, needSelectedPath, false);
  }

  public Project(
      Source source,
      List<String> patterns,
      TagFilter tagFilter,
      boolean needSelectedPath,
      boolean remainKey) {
    super(OperatorType.Project, source);
    if (patterns == null) {
      throw new IllegalArgumentException("patterns shouldn't be null");
    }
    this.patterns = patterns;
    this.tagFilter = tagFilter;
    this.needSelectedPath = needSelectedPath;
    this.remainKey = remainKey;
  }

  public List<String> getPatterns() {
    return patterns;
  }

  public TagFilter getTagFilter() {
    return tagFilter;
  }

  public boolean isRemainKey() {
    return remainKey;
  }

  public boolean isNeedSelectedPath() {
    return needSelectedPath;
  }

  public void setNeedSelectedPath(boolean needSelectedPath) {
    this.needSelectedPath = needSelectedPath;
  }

  @Override
  public Operator copy() {
    return new Project(
        getSource().copy(),
        new ArrayList<>(patterns),
        tagFilter == null ? null : tagFilter.copy(),
        needSelectedPath,
        remainKey);
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new Project(
        source,
        new ArrayList<>(patterns),
        tagFilter == null ? null : tagFilter.copy(),
        needSelectedPath,
        remainKey);
  }

  @Override
  public String getInfo() {
    StringBuilder builder = new StringBuilder();
    builder.append("Patterns: ");
    for (String pattern : patterns) {
      builder.append(pattern).append(",");
    }
    builder.deleteCharAt(builder.length() - 1);
    Source source = getSource();
    if (source.getType() == SourceType.Fragment) {
      FragmentMeta meta = ((FragmentSource) source).getFragment();
      String du = meta.getMasterStorageUnitId();
      builder.append(", Target DU: ").append(du);
    }
    if (tagFilter != null) {
      builder.append(", TagFilter: ").append(tagFilter.toString());
    }
    return builder.toString();
  }
}
