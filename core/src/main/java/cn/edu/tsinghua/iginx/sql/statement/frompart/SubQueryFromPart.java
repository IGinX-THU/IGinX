package cn.edu.tsinghua.iginx.sql.statement.frompart;

import cn.edu.tsinghua.iginx.sql.statement.SelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;

public class SubQueryFromPart implements FromPart {

    private final FromPartType type = FromPartType.SubQueryFromPart;
    private final SelectStatement subQuery;
    private final String alias;
    private final boolean isJoinPart;
    private JoinCondition joinCondition;

    public SubQueryFromPart(SelectStatement subQuery) {
        this.subQuery = subQuery;
        this.alias = subQuery.getGlobalAlias();
        this.isJoinPart = false;
    }

    public SubQueryFromPart(SelectStatement subQuery, JoinCondition joinCondition) {
        this.subQuery = subQuery;
        this.alias = subQuery.getGlobalAlias();
        this.isJoinPart = true;
        this.joinCondition = joinCondition;
    }

    @Override
    public FromPartType getType() {
        return type;
    }

    @Override
    public String getPath() {
        return alias;
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
}
