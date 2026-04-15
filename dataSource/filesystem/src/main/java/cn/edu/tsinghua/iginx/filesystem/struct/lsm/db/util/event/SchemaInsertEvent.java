package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.event;

import jdk.jfr.Event;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

@SuppressWarnings("Since15")
@Name("iginx.filesystem.filelsm.schema.insert")
@StackTrace(false)
public class SchemaInsertEvent extends Event {
    public int fieldsIndexed;
    public int fieldsInserted;
}
