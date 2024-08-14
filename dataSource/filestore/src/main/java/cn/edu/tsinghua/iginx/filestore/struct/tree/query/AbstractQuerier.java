package cn.edu.tsinghua.iginx.filestore.struct.tree.query;

import cn.edu.tsinghua.iginx.filestore.struct.DataTarget;
import java.nio.file.Path;
import lombok.Getter;

@Getter
public abstract class AbstractQuerier implements Querier {

  private final Path path;
  private final String prefix;
  private final DataTarget target;

  protected AbstractQuerier() {
    this(null, null, null);
  }

  protected AbstractQuerier(Path path, String prefix, DataTarget target) {
    this.path = path;
    this.prefix = prefix;
    this.target = target;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "://" + path + "?prefix=" + prefix + "&target=" + target;
  }
}
