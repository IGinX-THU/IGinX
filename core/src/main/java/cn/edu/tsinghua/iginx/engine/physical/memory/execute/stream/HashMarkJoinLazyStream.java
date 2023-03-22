package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.MarkJoin;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils.getJoinPathFromFilter;
import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils.constructNewHead;

public class HashMarkJoinLazyStream extends BinaryLazyStream {

	private final MarkJoin markJoin;
	private final HashMap<Integer, List<Row>> streamBHashMap;
	private final Deque<Row> cache;
	private Header header;
	private boolean hasInitialized = false;
	private String joinPathA;
	private boolean needTypeCast = false;

	public HashMarkJoinLazyStream(MarkJoin markJoin, RowStream streamA, RowStream streamB) {
		super(streamA, streamB);
		this.markJoin = markJoin;
		this.streamBHashMap = new HashMap<>();
		this.cache = new LinkedList<>();
	}

	private void initialize() throws PhysicalException {
		this.header = constructNewHead(streamA.getHeader(), markJoin.getMarkColumn());
		Pair<String, String> joinPath = getJoinPathFromFilter(markJoin.getFilter(), streamA.getHeader(), streamB.getHeader());
		this.joinPathA = joinPath.k;
		String joinPathB = joinPath.v;

		DataType dataType1 = streamA.getHeader().getField(streamA.getHeader().indexOf(joinPathA)).getType();
		DataType dataType2 = streamB.getHeader().getField(streamB.getHeader().indexOf(joinPathB)).getType();
		if (ValueUtils.isNumericType(dataType1) && ValueUtils.isNumericType(dataType2)) {
			this.needTypeCast = true;
		}

		while (streamB.hasNext()) {
			Row rowB = streamB.next();
			Value value = rowB.getAsValue(joinPathB);
			if (value == null) {
				continue;
			}
			if (needTypeCast) {
				value = ValueUtils.transformToDouble(value);
			}
			int hash;
			if (value.getDataType() == DataType.BINARY) {
				hash = Arrays.hashCode(value.getBinaryV());
			} else {
				hash = value.getValue().hashCode();
			}
			List<Row> rows = streamBHashMap.getOrDefault(hash, new ArrayList<>());
			rows.add(rowB);
			streamBHashMap.putIfAbsent(hash, rows);
		}
		this.hasInitialized = true;
	}

	@Override
	public Header getHeader() throws PhysicalException {
		if (!hasInitialized) {
			initialize();
		}
		return header;
	}

	@Override
	public boolean hasNext() throws PhysicalException {
		if (!hasInitialized) {
			initialize();
		}
		while (cache.isEmpty() && streamA.hasNext()) {
			tryMatch();
		}
		return !cache.isEmpty();
	}

	private void tryMatch() throws PhysicalException {
		Row rowA = streamA.next();

		Value value = rowA.getAsValue(joinPathA);
		if (value == null) {
			return;
		}
		if (needTypeCast) {
			value = ValueUtils.transformToDouble(value);
		}
		int hash;
		if (value.getDataType() == DataType.BINARY) {
			hash = Arrays.hashCode(value.getBinaryV());
		} else {
			hash = value.getValue().hashCode();
		}

		if (streamBHashMap.containsKey(hash)) {
			Row returnRow = RowUtils.constructNewRowWithMark(header, rowA, !markJoin.isAntiJoin());
			cache.add(returnRow);
		} else {
			Row unmatchedRow = RowUtils.constructNewRowWithMark(header, rowA, markJoin.isAntiJoin());
			cache.add(unmatchedRow);
		}
	}

	@Override
	public Row next() throws PhysicalException {
		if (!hasNext()) {
			throw new IllegalStateException("row stream doesn't have more data!");
		}
		return cache.pollFirst();
	}
}
