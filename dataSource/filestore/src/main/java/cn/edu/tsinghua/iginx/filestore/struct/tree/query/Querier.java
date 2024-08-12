package cn.edu.tsinghua.iginx.filestore.struct.tree.query;

import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface Querier extends Closeable {

  List<RowStream> query() throws IOException;

}
