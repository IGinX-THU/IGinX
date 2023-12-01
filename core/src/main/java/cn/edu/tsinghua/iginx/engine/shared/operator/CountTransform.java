package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

import java.util.ArrayList;
import java.util.List;

public class CountTransform extends AbstractUnaryOperator {
    private final List<String> expressionList;
    private final double funcParam;

    public CountTransform(Source source, List<String> expressionList, double funcParam) {
        super(OperatorType.CountTransform, source);
        if(expressionList.isEmpty()) {
            throw new IllegalArgumentException("expression list shouldn't be empty");
        }
        this.expressionList = expressionList;
        this.funcParam = funcParam;
    }

    public List<String> getExpressionList() {
        return expressionList;
    }
    public double getFuncParam() {
        return funcParam;
    }

    @Override
    public Operator copy() {
        return new CountTransform(getSource().copy(), new ArrayList<>(expressionList), funcParam);
    }

    @Override
    public UnaryOperator copyWithSource(Source source) {
        return new CountTransform(getSource().copy(), new ArrayList<>(expressionList), funcParam);
    }

    @Override
    public String getInfo() {
        return "Func: " + expressionList.toString();
    }
}
