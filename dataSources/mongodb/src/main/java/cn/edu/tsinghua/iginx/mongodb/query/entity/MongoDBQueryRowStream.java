package cn.edu.tsinghua.iginx.mongodb.query.entity;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.mongodb.MongoDBStorage;
import cn.edu.tsinghua.iginx.mongodb.tools.DataUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.mongodb.client.MongoCursor;
import org.apache.arrow.flatbuf.Bool;
import org.bson.Document;
import org.bson.types.Binary;

import java.util.*;

public class MongoDBQueryRowStream implements RowStream {

    private final Table table;

    public MongoDBQueryRowStream(MongoCursor<Document> cursor) {
        // TODO: @zhanglingzhe
        this.table = null;
    }

    @Override
    public Header getHeader() {
        return table.getHeader();
    }

    @Override
    public void close() {
        table.close();
    }

    @Override
    public boolean hasNext() {
        return table.hasNext();
    }

    @Override
    public Row next() {
        return table.next();
    }
}
