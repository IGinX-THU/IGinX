package cn.edu.tsinghua.iginx.engine.shared.source;

import java.util.ArrayList;
import java.util.List;

public class ConstantSource extends AbstractSource {
    private List<String> expressionList = new ArrayList<>();

    public ConstantSource() {
        super(SourceType.Constant);
    }

    public ConstantSource(List<String> expressionList) {
        super(SourceType.Constant);
        this.expressionList = expressionList;
    }

    public List<String> getExpressionList() {
        return expressionList;
    }

    @Override
    public Source copy() {
        return new ConstantSource(expressionList);
    }
}
