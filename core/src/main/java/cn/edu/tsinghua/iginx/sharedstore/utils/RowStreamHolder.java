package cn.edu.tsinghua.iginx.sharedstore.utils;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;

import java.util.ArrayList;
import java.util.List;

public class RowStreamHolder {

    private RowStream stream;

    private Table table;

    public RowStreamHolder(RowStream stream) {
        this.stream = stream;
    }

    public RowStream getStream() {
        if (this.stream != null) {
            RowStream stream = this.stream;
            this.stream = null;
            return stream;
        }
        if (this.table != null) {
            Table table = this.table;
            this.table = null;
            return table;
        }
        return null;
    }

    public Table getAsTable() throws PhysicalException {
        return transformToTable(getStream());
    }

    protected void setTable(Table table) {
        this.table = table;
    }

    private Table transformToTable(RowStream stream) throws PhysicalException {
        if (stream instanceof Table) {
            return (Table) stream;
        }
        Header header = stream.getHeader();
        List<Row> rows = new ArrayList<>();
        while (stream.hasNext()) {
            rows.add(stream.next());
        }
        stream.close();
        return new Table(header, rows);
    }

}
