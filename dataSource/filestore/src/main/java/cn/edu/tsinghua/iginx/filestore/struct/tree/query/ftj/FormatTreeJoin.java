package cn.edu.tsinghua.iginx.filestore.struct.tree.query.ftj;

import cn.edu.tsinghua.iginx.filestore.struct.tree.FileTreeConfig;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Querier.Builder;
import cn.edu.tsinghua.iginx.filestore.struct.tree.query.Querier.Builder.Factory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.annotation.Nullable;

public class FormatTreeJoin implements Factory {

  private final Factory forRegularFile = new FormatQuerierBuilderFactory();
  private final Factory forDirectory = new TreeJoinQuerierBuilderFactory(this);

  @Override
  public Builder create(@Nullable String prefix, Path path, FileTreeConfig config)
      throws IOException {
    if (Files.isDirectory(path)) {
      return forDirectory.create(prefix, path, config);
    } else if (Files.isRegularFile(path)) {
      return forRegularFile.create(prefix, path, config);
    } else if (!Files.exists(path)) {
      throw new IOException("file does not exist: " + path);
    } else {
      throw new IllegalArgumentException("Unsupported file type: " + path);
    }
  }
}
