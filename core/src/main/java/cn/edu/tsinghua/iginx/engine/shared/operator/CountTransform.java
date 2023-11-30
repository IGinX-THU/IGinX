package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.sql.expression.Expression;

import java.util.ArrayList;
import java.util.List;

public class CountTransform extends AbstractUnaryOperator {
    private  List<String> expressionList;

    public CountTransform(Source source, List<String> expressionList) {
        super(OperatorType.CountTransform, source);
        if(expressionList.isEmpty()) {
            throw new IllegalArgumentException("expression list shouldn't be empty");
        }
        this.expressionList = expressionList;
    }

    public List<String> getExpressionList() {
        return expressionList;
    }

    @Override
    public Operator copy() {
        return new CountTransform(getSource().copy(), new ArrayList<>(expressionList));
    }

    @Override
    public UnaryOperator copyWithSource(Source source) {
        return new CountTransform(getSource().copy(), new ArrayList<>(expressionList));
    }

    @Override
    public String getInfo() {
        return "Func: " + expressionList.toString();
    }
}
