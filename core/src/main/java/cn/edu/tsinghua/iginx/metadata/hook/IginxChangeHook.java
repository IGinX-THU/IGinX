package cn.edu.tsinghua.iginx.metadata.hook;

import cn.edu.tsinghua.iginx.metadata.entity.IginxMeta;

public interface IginxChangeHook {

  void onChange(long id, IginxMeta iginx);
}
