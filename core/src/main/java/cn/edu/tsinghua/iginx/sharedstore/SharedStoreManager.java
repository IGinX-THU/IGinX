package cn.edu.tsinghua.iginx.sharedstore;

import cn.edu.tsinghua.iginx.sharedstore.redis.RedisStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class SharedStoreManager {

    private static final Logger logger = LoggerFactory.getLogger(SharedStoreManager.class);

    private static final SharedStoreManager instance = new SharedStoreManager();

    private final Map<String, SharedStore> storeCache;

    private SharedStoreManager() {
        storeCache = new HashMap<>();
    }

    public synchronized SharedStore getSharedStore(String name) {
        if (storeCache.containsKey(name)) {
            return storeCache.get(name);
        }
        SharedStore sharedStore = null;
        switch (name) {
            case "redis":
                sharedStore = RedisStore.getInstance();
                break;
            default:
                logger.error("unknown shared store: " + name);
                return null;
        }
        storeCache.put(name, sharedStore);
        return sharedStore;
    }


    public static SharedStoreManager getInstance() {
        return instance;
    }

}
