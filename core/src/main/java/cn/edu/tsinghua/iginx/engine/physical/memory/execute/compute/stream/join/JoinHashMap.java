package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.stream.join;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.row.MaterializedRowKey;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.row.RowCursor;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.hash.Hasher;

public class JoinHashMap {
  private final Multimap<MaterializedRowKey, RowCursor> hashCodeToRowCursor;

  public JoinHashMap() {
    this.hashCodeToRowCursor = MultimapBuilder.hashKeys().arrayListValues().build();
  }

}
