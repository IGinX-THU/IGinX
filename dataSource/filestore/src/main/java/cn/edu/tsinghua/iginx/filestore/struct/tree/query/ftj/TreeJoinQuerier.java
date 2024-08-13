package cn.edu.tsinghua.iginx.filestore.struct.tree.query.ftj;

import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.filestore.common.Closeables;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Querier;

import javax.annotation.WillCloseWhenClosed;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

class TreeJoinQuerier implements Querier {

  private final List<? extends Querier> queriers;

  public TreeJoinQuerier(@WillCloseWhenClosed List<? extends Querier> queriers) {
    this.queriers = Objects.requireNonNull(queriers);
  }

  @Override
  public void close() throws IOException {
    Closeables.close(queriers);
  }

  @Override
  public String toString() {
    return "TreeJoinQuerier{" +
        "queriers=" + queriers +
        '}';
  }

  @Override
  public List<RowStream> query() throws IOException {
    List<RowStream> rowStreams = new ArrayList<>();
    for (Querier querier : queriers) {
      rowStreams.addAll(querier.query());
    }
    return rowStreams;
  }

}
