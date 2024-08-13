package cn.edu.tsinghua.iginx.filestore.struct.tree.query;

import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;
import cn.edu.tsinghua.iginx.filestore.struct.tree.FileTreeConfig;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public interface Querier extends Closeable {

  List<RowStream> query() throws IOException;

  interface Builder extends Closeable {
    Querier build(DataTarget parentTarget) throws IOException;

    interface Factory {
      Builder create(@Nullable String prefix, Path path, FileTreeConfig config) throws IOException;
    }
  }

}
