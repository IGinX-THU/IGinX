package cn.edu.tsinghua.iginx.sql.statement.join;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class JoinCondition {

    private final JoinType joinType;
    private final Filter filter;
    private final List<String> joinColumns;

    public JoinCondition() {
        this(JoinType.CrossJoin, null, Collections.emptyList());
    }

    public JoinCondition(JoinType joinType, Filter filter, List<String> joinColumns) {
        this.joinType = joinType;
        this.filter = filter;
        this.joinColumns = joinColumns;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public Filter getFilter() {
        return filter;
    }

    public List<String> getJoinColumns() {
        return joinColumns;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JoinCondition joinCondition = (JoinCondition) o;
        return joinType == joinCondition.joinType && Objects.equals(filter, joinCondition.filter)
            && Objects.equals(joinColumns, joinCondition.joinColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(joinType, filter, joinColumns);
    }
}
