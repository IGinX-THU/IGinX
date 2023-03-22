package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.SingleJoin;

import java.util.ArrayList;
import java.util.List;

public class NestedLoopSingleJoinLazyStream extends BinaryLazyStream {

	private final SingleJoin singleJoin;
	private final List<Row> streamBCache;
	private final List<Row> unmatchedStreamARows;
	private final List<Row> lastPart;
	private Header header;
	private boolean curNextAHasMatched = false;
	private int curStreamBIndex = 0;
	private boolean hasInitialized = false;
	private boolean lastPartHasInitialized = false;
	private int lastPartIndex = 0;
	private Row nextA;
	private Row nextB;
	private Row nextRow;

	public NestedLoopSingleJoinLazyStream(SingleJoin singleJoin, RowStream streamA, RowStream streamB) {
		super(streamA, streamB);
		this.singleJoin = singleJoin;
		this.unmatchedStreamARows = new ArrayList<>();
		this.streamBCache = new ArrayList<>();
		this.lastPart = new ArrayList<>();
	}

	private void initialize() throws PhysicalException {
		if (hasInitialized) {
			return;
		}
		this.header = RowUtils.constructNewHead(streamA.getHeader(), streamB.getHeader(), true);
		this.hasInitialized = true;
	}

	private void initializeLastPart() throws PhysicalException {
		if (lastPartHasInitialized) {
			return;
		}
		int anotherRowSize = streamB.getHeader().getFieldSize();
		for (Row halfRow : unmatchedStreamARows) {
			Row unmatchedRow = RowUtils.constructUnmatchedRow(header, halfRow, anotherRowSize, true);
			lastPart.add(unmatchedRow);
		}
		this.lastPartHasInitialized = true;
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
		if (nextRow != null) {
			return true;
		}
		while (nextRow == null && hasMoreRows()) {
			nextRow = tryMatch();
		}
		if (nextRow == null) {
			initializeLastPart();
			if (lastPartIndex < lastPart.size()) {
				nextRow = lastPart.get(lastPartIndex);
				lastPartIndex++;
			}
		}
		return nextRow != null;
	}

	private boolean hasMoreRows() throws PhysicalException {
		if (!hasInitialized) {
			initialize();
		}
		if (streamA.hasNext()) {
			return true;
		} else {
			if (curStreamBIndex < streamBCache.size()) {
				return true;
			} else {
				if (nextA != null && !curNextAHasMatched) {
					unmatchedStreamARows.add(nextA);
					nextA = null;
				}
				return false;
			}
		}
	}

	private Row tryMatch() throws PhysicalException {
		if (!hasMoreRows()) {
			return null;
		}
		if (nextA == null && streamA.hasNext()) {
			nextA = streamA.next();
		}
		if (nextB == null) {
			if (streamB.hasNext()) {
				nextB = streamB.next();
				streamBCache.add(nextB);
			} else if (curStreamBIndex < streamBCache.size()) {
				nextB = streamBCache.get(curStreamBIndex);
			} else {
				if (!curNextAHasMatched) {
					unmatchedStreamARows.add(nextA);
				}
				nextA = streamA.next();
				curNextAHasMatched = false;
				curStreamBIndex = 0;
				nextB = streamBCache.get(curStreamBIndex);
			}
			curStreamBIndex++;
		}
		
		Row row = RowUtils.constructNewRow(header, nextA, nextB, true);
		nextB = null;
		if (FilterUtils.validate(singleJoin.getFilter(), row)) {
			if (!this.curNextAHasMatched) {
				this.curNextAHasMatched = true;
				return row;
			} else {
				throw new PhysicalException("the return value of sub-query has more than one rows");
			}
		} else {
			return null;
		}
	}

	@Override
	public Row next() throws PhysicalException {
		if (!hasNext()) {
			throw new IllegalStateException("row stream doesn't have more data!");
		}
		Row ret = nextRow;
		nextRow = null;
		return ret;
	}
}
