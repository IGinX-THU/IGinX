package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.event;

import jdk.jfr.Event;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

@SuppressWarnings("Since15")
@Name("iginx.filesystem.filelsm.table.append")
@StackTrace(false)
public class TableAppendEvent extends Event {

    public String activeMemTableName;

    public long memTableId;
}
