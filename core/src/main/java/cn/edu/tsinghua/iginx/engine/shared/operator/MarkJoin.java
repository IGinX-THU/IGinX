package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class MarkJoin extends AbstractBinaryOperator {

	private final Filter filter;
	private final String markColumn;
	private final boolean isAntiJoin;

	public MarkJoin(Source sourceA, Source sourceB, Filter filter, String markColumn, boolean isAntiJoin) {
		super(OperatorType.MarkJoin, sourceA, sourceB);
		this.filter = filter;
		this.markColumn = markColumn;
		this.isAntiJoin = isAntiJoin;
	}

	public Filter getFilter() {
		return filter;
	}

	public String getMarkColumn() {
		return markColumn;
	}

	public boolean isAntiJoin() {
		return isAntiJoin;
	}

	@Override
	public Operator copy() {
		return new MarkJoin(getSourceA().copy(), getSourceB().copy(), filter.copy(), markColumn, isAntiJoin);
	}

	@Override
	public String getInfo() {
		return "Filter: " + filter.toString() + ", MarkColumn: " + markColumn + ", IsAntiJoin: " + isAntiJoin;
	}
}
