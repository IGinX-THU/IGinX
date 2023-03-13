package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class AntiMarkJoin extends AbstractBinaryOperator {

	private final Filter filter;
	private final String markColumn;

	public AntiMarkJoin(Source sourceA, Source sourceB, Filter filter, String markColumn) {
		super(OperatorType.AntiMarkJoin, sourceA, sourceB);
		this.filter = filter;
		this.markColumn = markColumn;
	}

	public Filter getFilter() {
		return filter;
	}

	public String getMarkColumn() {
		return markColumn;
	}

	@Override
	public Operator copy() {
		return new AntiMarkJoin(getSourceA().copy(), getSourceB().copy(), filter.copy(), markColumn);
	}

	@Override
	public String getInfo() {
		return "Filter: " + filter.toString() + ", MarkColumn: " + markColumn;
	}
}
