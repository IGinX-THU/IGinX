package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class GroupBy extends AbstractUnaryOperator {

    private final List<String> groupByCols;

    private final List<FunctionCall> functionCallList;

    public GroupBy(Source source, List<String> groupByCols, List<FunctionCall> functionCallList) {
        super(OperatorType.GroupBy, source);
        this.groupByCols = groupByCols;
        this.functionCallList = functionCallList;
    }

    public List<String> getGroupByCols() {
        return groupByCols;
    }

    public List<FunctionCall> getFunctionCallList() {
        return functionCallList;
    }

    @Override
    public Operator copy() {
        return new GroupBy(getSource().copy(), new ArrayList<>(groupByCols), new ArrayList<>(functionCallList));
    }

    @Override
    public String getInfo() {
        return "";
    }
}
