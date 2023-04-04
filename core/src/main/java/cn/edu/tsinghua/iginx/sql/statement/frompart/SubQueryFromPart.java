package cn.edu.tsinghua.iginx.sql.statement.frompart;

import cn.edu.tsinghua.iginx.sql.statement.SelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;
import java.util.ArrayList;
import java.util.List;

public class SubQueryFromPart implements FromPart {

    private final FromPartType type = FromPartType.SubQueryFromPart;
    private final SelectStatement subQuery;
    private final String alias;
    private final boolean isJoinPart;
    private JoinCondition joinCondition;
    private List<String> freeVariables;

    public SubQueryFromPart(SelectStatement subQuery) {
        this.subQuery = subQuery;
        this.alias = subQuery.getGlobalAlias();
        this.isJoinPart = false;
        this.freeVariables = new ArrayList<>();
    }

    public SubQueryFromPart(SelectStatement subQuery, JoinCondition joinCondition) {
        this.subQuery = subQuery;
        this.alias = subQuery.getGlobalAlias();
        this.isJoinPart = true;
        this.joinCondition = joinCondition;
        this.freeVariables = new ArrayList<>();
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

    @Override
    public List<String> getFreeVariables() {
        return freeVariables;
    }

    public void setFreeVariables(List<String> freeVariables) {
        this.freeVariables = freeVariables;
    }
}
