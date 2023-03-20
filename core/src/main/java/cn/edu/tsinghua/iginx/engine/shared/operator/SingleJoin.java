package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class SingleJoin extends AbstractBinaryOperator{

	private final Filter filter;
	private final JoinAlgType joinAlgType;
	
	public SingleJoin(Source sourceA, Source sourceB, Filter filter, JoinAlgType joinAlgType) {
		super(OperatorType.SingleJoin, sourceA, sourceB);
		this.filter = filter;
		this.joinAlgType = joinAlgType;
	}

	public Filter getFilter() {
		return filter;
	}

	public JoinAlgType getJoinAlgType() {
		return joinAlgType;
	}
	
	@Override
	public Operator copy() {
		return new SingleJoin(getSourceA().copy(), getSourceB().copy(), filter.copy(), joinAlgType);
	}
	
	@Override
	public String getInfo() {
		return "Filter: " + filter.toString();
	}
}
