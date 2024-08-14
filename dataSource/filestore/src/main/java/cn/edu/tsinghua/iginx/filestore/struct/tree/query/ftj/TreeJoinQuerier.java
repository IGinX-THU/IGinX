package cn.edu.tsinghua.iginx.filestore.struct.tree.query.ftj;

import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.filestore.common.Closeables;
import cn.edu.tsinghua.iginx.filestore.common.Strings;
import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.AbstractQuerier;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Querier;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

class TreeJoinQuerier extends AbstractQuerier {

  private final List<Querier> queriers = new ArrayList<>();

  TreeJoinQuerier(Path path, String prefix, DataTarget target) {
    super(path, prefix, target);
  }

  public void add(Querier querier) {
    queriers.add(querier);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(super.toString()).append("&queriers=");
    for (Querier querier : queriers) {
      sb.append(Strings.shiftWithNewline(querier.toString()));
    }
    return sb.toString();
  }

  @Override
  public void close() throws IOException {
    Closeables.close(queriers);
    queriers.clear();
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
