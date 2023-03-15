package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class MarkJoin extends AbstractBinaryOperator {

	private final Filter filter;
	private final String markColumn;
	private final boolean isAntiJoin;
	private final JoinAlgType joinAlgType;

	public MarkJoin(Source sourceA, Source sourceB, Filter filter, String markColumn, boolean isAntiJoin, JoinAlgType joinAlgType) {
		super(OperatorType.MarkJoin, sourceA, sourceB);
		this.filter = filter;
		this.markColumn = markColumn;
		this.isAntiJoin = isAntiJoin;
		this.joinAlgType = joinAlgType;
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

	public JoinAlgType getJoinAlgType() {
		return joinAlgType;
	}

	@Override
	public Operator copy() {
		return new MarkJoin(getSourceA().copy(), getSourceB().copy(), filter.copy(), markColumn, isAntiJoin, joinAlgType);
	}

	@Override
	public String getInfo() {
		return "Filter: " + filter.toString() + ", MarkColumn: " + markColumn + ", IsAntiJoin: " + isAntiJoin;
	}
}
