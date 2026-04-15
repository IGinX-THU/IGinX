package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.event;

import jdk.jfr.Event;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

@SuppressWarnings("Since15")
@Name("iginx.filesystem.filelsm.schema.remove")
@StackTrace(false)
public class SchemaRemoveEvent extends Event {
    public int fieldsIndexed;
    public int fieldsRemoved;
}
