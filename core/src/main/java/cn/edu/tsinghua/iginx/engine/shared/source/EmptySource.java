package cn.edu.tsinghua.iginx.engine.shared.source;

public class EmptySource implements Source {

  public static final EmptySource EMPTY_SOURCE = new EmptySource();

  @Override
  public SourceType getType() {
    return null;
  }

  @Override
  public Source copy() {
    return null;
  }
}
