package cn.edu.tsinghua.iginx.filestore.struct.tree.query.ftj;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.filestore.common.Filters;
import cn.edu.tsinghua.iginx.filestore.format.FileFormat;
import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Querier;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Querier.Builder;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Queriers;
import com.typesafe.config.Config;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;

class FormatQuerierBuilder implements Builder {

  @Nullable
  private final String prefix;
  private final Path path;
  private final FileFormat format;
  private final Config config;

  FormatQuerierBuilder(@Nullable String prefix, Path path, FileFormat format, Config config) {
    this.format = format;
    this.prefix = prefix;
    this.path = path;
    this.config = config;
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public Querier build(DataTarget target) throws IOException {
    // TODO: 下推更多类型的过滤器
    Filter filter = Filters.superSet(target.getFilter(), Filters.nonKeyFilter());

    FileFormat.Reader reader = format.newReader(prefix, path, config);

    FormatQuerier querier = new FormatQuerier(reader, target.withFilter(filter));
    if (Filters.match(target.getFilter(), Filters.nonKeyFilter())) {
      return querier;
    }
    return Queriers.filtered(querier, target.getFilter());
  }


}
