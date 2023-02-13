package cn.edu.tsinghua.iginx.sharedstore.utils;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.sharedstore.SharedStore;
import cn.edu.tsinghua.iginx.sharedstore.SharedStoreManager;
import com.alibaba.fastjson2.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class QueryStoreUtils {

    private static final Logger logger = LoggerFactory.getLogger(QueryStoreUtils.class);

    private static final SharedStore sharedStore = SharedStoreManager.getInstance().getSharedStore(ConfigDescriptor.getInstance().getConfig().getSharedStorage());

    public static boolean storeQueryContext(RequestContext context){
        long queryId = context.getId();
        byte[] bytes = JSON.toJSONBytes(context);
        return sharedStore.put(Long.toString(queryId).getBytes(StandardCharsets.UTF_8), bytes);
    }

    public static RequestContext loadQueryContext(long queryId) {
        byte[] bytes = sharedStore.get(Long.toString(queryId).getBytes(StandardCharsets.UTF_8));
        if (bytes == null) {
            logger.info("query context for key: " + queryId + " is not exists.");
            return null;
        }
        return JSON.parseObject(bytes, RequestContext.class);
    }

}
