package cn.edu.tsinghua.iginx.sql.statement.frompart;

import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;

public class PathFromPart implements FromPart {

    private final FromPartType type = FromPartType.PathFromPart;
    private final String path;
    private final boolean isJoinPart;
    private JoinCondition joinCondition;

    public PathFromPart(String path) {
        this.path = path;
        this.isJoinPart = false;
    }

    public PathFromPart(String path, JoinCondition joinCondition) {
        this.path = path;
        this.joinCondition = joinCondition;
        this.isJoinPart = true;
    }

    @Override
    public FromPartType getType() {
        return type;
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public boolean isJoinPart() {
        return isJoinPart;
    }

    @Override
    public JoinCondition getJoinCondition() {
        return joinCondition;
    }
}
