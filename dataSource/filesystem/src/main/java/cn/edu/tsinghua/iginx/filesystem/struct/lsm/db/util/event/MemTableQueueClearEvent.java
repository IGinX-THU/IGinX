package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.event;

import jdk.jfr.DataAmount;
import jdk.jfr.Event;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

@SuppressWarnings("Since15")
@Name("iginx.filesystem.filelsm.memtable.queue.clear")
@StackTrace(false)
public class MemTableQueueClearEvent extends Event {
    @DataAmount
    public long allocatedMemory;
}
