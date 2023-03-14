package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class SingleJoin extends AbstractBinaryOperator{

	private final Filter filter;
	
	public SingleJoin(Source sourceA, Source sourceB, Filter filter) {
		super(OperatorType.SingleJoin, sourceA, sourceB);
		this.filter = filter;
	}

	public Filter getFilter() {
		return filter;
	}
	
	@Override
	public Operator copy() {
		return new SingleJoin(getSourceA().copy(), getSourceB().copy(), filter.copy());
	}
	
	@Override
	public String getInfo() {
		return "Filter: " + filter.toString();
	}
}
