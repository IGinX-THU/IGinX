package cn.edu.tsinghua.iginx.engine.shared.operator.tag;

public interface TagFilter {

  TagFilterType getType();

  TagFilter copy();
}
