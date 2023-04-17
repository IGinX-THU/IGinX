package cn.edu.tsinghua.iginx.sql.statement.frompart;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.ALL_PATH_SUFFIX;

import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
    public boolean hasSinglePrefix() {
        return true;
    }

    @Override
    public List<String> getPatterns() {
        return new ArrayList<>(Collections.singleton(path + ALL_PATH_SUFFIX));
    }

    @Override
    public String getPrefix() {
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

    @Override
    public List<String> getFreeVariables() {
        return new ArrayList<>();
    }
}
