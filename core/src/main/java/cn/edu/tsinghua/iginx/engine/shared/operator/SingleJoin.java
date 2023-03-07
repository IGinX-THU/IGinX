package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class SingleJoin extends AbstractBinaryOperator{

	private final String prefixA;
	private final Filter filter;
	
	public SingleJoin(Source sourceA, Source sourceB, String prefixA, Filter filter) {
		super(OperatorType.SingleJoin, sourceA, sourceB);
		this.prefixA = prefixA;
		this.filter = filter;
	}

	public String getPrefixA() {
		return prefixA;
	}
	public Filter getFilter() {
		return filter;
	}
	
	@Override
	public Operator copy() {
		return new SingleJoin(getSourceA().copy(), getSourceB().copy(), prefixA, filter.copy());
	}
	
	@Override
	public String getInfo() {
		return "PrefixA: " + prefixA + " Filter: " + filter.toString();
	}
}
