package cn.edu.tsinghua.iginx.filesystem.struct.lsm.db.util.event;

import jdk.jfr.*;

@SuppressWarnings("Since15")
@Name("iginx.filesystem.filelsm.table.flush")
@StackTrace(false)
public class TableFlushEvent extends Event {

  public long tableId;

  public String format;

  @DataAmount
  public long spaceUsed;
}
