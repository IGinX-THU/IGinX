package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.event;

import jdk.jfr.Event;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;
@SuppressWarnings("Since15")
@Name("iginx.filesystem.filelsm.table.read")
@StackTrace(false)
public class TableReadEvent extends Event {

    public String tableName;

    public String projectedSchema;

    public int numRows;
}
