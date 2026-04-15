package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.event;

import jdk.jfr.Event;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

@SuppressWarnings("Since15")
@Name("iginx.filesystem.filelsm.schema.find")
@StackTrace(false)
public class SchemaFindEvent extends Event {
    public int patternNum;
    public int fieldsIndexed;
    public int fieldsFound;
}
