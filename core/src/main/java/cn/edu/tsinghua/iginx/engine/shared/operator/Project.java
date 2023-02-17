package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import java.util.ArrayList;
import java.util.List;

public class Project extends AbstractUnaryOperator {

    private final List<String> patterns;

    private final TagFilter tagFilter;

    public Project(Source source, List<String> patterns, TagFilter tagFilter) {
        super(OperatorType.Project, source);
        if (patterns == null) {
            throw new IllegalArgumentException("patterns shouldn't be null");
        }
        this.patterns = patterns;
        this.tagFilter = tagFilter;
    }

    public List<String> getPatterns() {
        return patterns;
    }

    public TagFilter getTagFilter() {
        return tagFilter;
    }

    @Override
    public Operator copy() {
        return new Project(getSource().copy(), new ArrayList<>(patterns), tagFilter == null ? null : tagFilter.copy());
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
           FragmentMeta meta = ((FragmentSource)source).getFragment();
           String du = meta.getMasterStorageUnitId();
           builder.append(", Target DU: ").append(du);
        }
        if (tagFilter != null) {
            builder.append(", TagFilter: ").append(tagFilter.toString());
        }
        return builder.toString();
    }
}
