package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.event;

import jdk.jfr.DataAmount;
import jdk.jfr.Event;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

@SuppressWarnings("Since15")
@Name("iginx.filesystem.filelsm.schema.clear")
@StackTrace(false)
public class SchemaClearEvent extends Event {
    public int fieldsIndexed;
    @DataAmount
    public long allocatedMemory;
}
