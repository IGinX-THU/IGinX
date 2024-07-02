package cn.edu.tsinghua.iginx.engine.shared.source;

public abstract class AbstractSource implements Source {

  private final SourceType type;

  public AbstractSource(SourceType type) {
    if (type == null) {
      throw new IllegalArgumentException("source type shouldn't be null");
    }
    this.type = type;
  }

  public AbstractSource() {
    this.type = SourceType.Unknown;
  }

  @Override
  public SourceType getType() {
    return type;
  }
}
