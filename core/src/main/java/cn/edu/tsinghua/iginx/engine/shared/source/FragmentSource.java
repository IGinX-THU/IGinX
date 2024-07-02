package cn.edu.tsinghua.iginx.engine.shared.source;

import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;

public class FragmentSource extends AbstractSource {

  private final FragmentMeta fragment;

  public FragmentSource(FragmentMeta fragment) {
    super(SourceType.Fragment);
    if (fragment == null) {
      throw new IllegalArgumentException("fragment shouldn't be null");
    }
    this.fragment = fragment;
  }

  public FragmentMeta getFragment() {
    return fragment;
  }

  @Override
  public Source copy() {
    return new FragmentSource(fragment);
  }
}
