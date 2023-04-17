package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class Distinct extends AbstractUnaryOperator {

    public Distinct(Source source) {
        super(OperatorType.Distinct, source);
    }

    @Override
    public Operator copy() {
        return new Distinct(getSource().copy());
    }

    @Override
    public String getInfo() {
        return "";
    }
}
