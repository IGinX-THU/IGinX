package cn.edu.tsinghua.iginx.metadata.hook;

import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;

public interface FragmentChangeHook {

  void onChange(boolean create, FragmentMeta fragment);
}
