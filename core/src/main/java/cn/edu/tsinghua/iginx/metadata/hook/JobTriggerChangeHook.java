package cn.edu.tsinghua.iginx.metadata.hook;

import cn.edu.tsinghua.iginx.transform.pojo.TriggerDescriptor;

public interface JobTriggerChangeHook {
  void onChange(String className, TriggerDescriptor descriptor);
}
