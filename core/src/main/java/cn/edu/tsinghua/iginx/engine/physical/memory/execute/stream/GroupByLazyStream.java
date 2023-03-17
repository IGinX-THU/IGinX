package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.GroupBy;
import java.util.Deque;

public class GroupByLazyStream extends UnaryLazyStream {

    private final GroupBy groupBy;

    private Deque<Row> cache;

    private Header header;

    public GroupByLazyStream(GroupBy groupBy, RowStream stream) {
        super(stream);
        this.groupBy = groupBy;
    }

    @Override
    public Header getHeader() throws PhysicalException {
        if (header == null) {
            cacheResult();
        }
        return header;
    }

    @Override
    public boolean hasNext() throws PhysicalException {
        if (header == null) {
            cacheResult();
        }
        return cache.isEmpty();
    }

    @Override
    public Row next() throws PhysicalException {
        if (!hasNext()) {
            throw new IllegalStateException("row stream doesn't have more data!");
        }
        return cache.pollFirst();
    }

    private void cacheResult() throws PhysicalException {
        this.cache = RowUtils.cacheGroupByResult(groupBy, stream);
        if (cache.isEmpty()) {
            header = Header.EMPTY_HEADER;
        } else {
            header = cache.peekFirst().getHeader();
        }
    }
}
