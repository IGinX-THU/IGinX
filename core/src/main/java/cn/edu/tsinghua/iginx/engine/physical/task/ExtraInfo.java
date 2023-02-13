package cn.edu.tsinghua.iginx.engine.physical.task;

import java.util.Map;

public interface ExtraInfo {

    Object getExtraInfo(String key);

    void setExtraInfo(String key, Object value);

}
