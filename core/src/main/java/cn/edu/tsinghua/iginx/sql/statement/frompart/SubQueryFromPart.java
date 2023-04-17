package cn.edu.tsinghua.iginx.sql.statement.frompart;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.ALL_PATH_SUFFIX;

import cn.edu.tsinghua.iginx.sql.statement.SelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;
import java.util.List;

public class SubQueryFromPart implements FromPart {

    private final FromPartType type = FromPartType.SubQueryFromPart;
    private final SelectStatement subQuery;
    private final List<String> patterns;
    private final boolean isJoinPart;
    private JoinCondition joinCondition;

    public SubQueryFromPart(SelectStatement subQuery) {
        this.subQuery = subQuery;
        this.patterns = subQuery.calculatePrefixSet();
        this.isJoinPart = false;
    }

    public SubQueryFromPart(SelectStatement subQuery, JoinCondition joinCondition) {
        this.subQuery = subQuery;
        this.patterns = subQuery.calculatePrefixSet();
        this.isJoinPart = true;
        this.joinCondition = joinCondition;
    }

    @Override
    public FromPartType getType() {
        return type;
    }

    @Override
    public boolean hasSinglePrefix() {
        return patterns.size() == 1;
    }

    @Override
    public List<String> getPatterns() {
        return patterns;
    }

    @Override
    public String getPrefix() {
        if (hasSinglePrefix()) {
            if (patterns.get(0).endsWith(ALL_PATH_SUFFIX)) {
                return patterns.get(0).substring(0, patterns.get(0).length() - 2);
            } else {
                return patterns.get(0);
            }
        } else {
            return null;
        }
    }

    @Override
    public boolean isJoinPart() {
        return isJoinPart;
    }

    @Override
    public JoinCondition getJoinCondition() {
        return joinCondition;
    }

    public SelectStatement getSubQuery() {
        return subQuery;
    }

    @Override
    public List<String> getFreeVariables() {
        return subQuery.getFreeVariables();
    }
}
