package cn.edu.tsinghua.iginx.filestore.struct.tree.query;

import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;

import javax.annotation.concurrent.Immutable;
import java.io.Closeable;
import java.io.IOException;

@Immutable
public interface QuerierBuilder extends Closeable {
  Querier build(DataTarget target) throws IOException;
}
