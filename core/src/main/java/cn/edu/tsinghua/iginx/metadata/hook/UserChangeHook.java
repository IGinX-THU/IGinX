package cn.edu.tsinghua.iginx.metadata.hook;

import cn.edu.tsinghua.iginx.metadata.entity.UserMeta;

public interface UserChangeHook {

  void onChange(String username, UserMeta user);
}
