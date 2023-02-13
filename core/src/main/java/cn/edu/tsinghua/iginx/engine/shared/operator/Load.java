package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.SharedStoreSource;

public class Load extends AbstractUnaryOperator {

    private final String key;

    public Load(String key) {
        super(OperatorType.Load, new SharedStoreSource(key));
        this.key = key;
    }

    @Override
    public Operator copy() {
        return new Load(key);
    }

    public String getKey() {
        return key;
    }

    @Override
    public String getInfo() {
        return "Load: " + key;
    }
}
