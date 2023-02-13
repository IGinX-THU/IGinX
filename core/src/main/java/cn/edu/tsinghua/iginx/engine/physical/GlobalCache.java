package cn.edu.tsinghua.iginx.engine.physical;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class GlobalCache {

    public static final Map<String, Long> storageUnitLastTs = new ConcurrentHashMap<>();

}
