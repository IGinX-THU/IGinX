package cn.edu.tsinghua.iginx.engine.shared.source;

public interface Source {

  SourceType getType();

  Source copy();
}
