package cn.edu.tsinghua.iginx.engine.physical.fault;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.sharedstore.utils.RowStreamHolder;
import cn.edu.tsinghua.iginx.sharedstore.utils.RowStreamStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractFaultTolerancePolicy implements FaultTolerancePolicy {

    private static final Logger logger = LoggerFactory.getLogger(AbstractFaultTolerancePolicy.class);

    protected abstract boolean needPersistence(PhysicalTask task, TaskExecuteResult result);

    private final boolean enableSharedStorage = ConfigDescriptor.getInstance().getConfig().isEnableSharedStorage();

    @Override
    public void persistence(PhysicalTask task, TaskExecuteResult result) {
        if (!enableSharedStorage) {
            logger.info("[LongQuery][AbstractFaultTolerancePolicy] enableSharedStorage is false, so we skip persistence.");
            return;
        }
        if (task.getContext() == null) {
            logger.info("[LongQuery][AbstractFaultTolerancePolicy] physical task context is false, so we skip persistence.");
            return;
        }
        String key = RowStreamStoreUtils.encodeKey(task.getContext().getId(), task.getOperators().get(0).getSequence());
        if (!needPersistence(task, result)) {
            return;
        }
        RowStreamHolder holder = new RowStreamHolder(result.getRowStream());
        try {
            if (!RowStreamStoreUtils.storeRowStream(key, holder)) {
                logger.error("[LongQuery][AbstractFaultTolerancePolicy][key={}] persistence failure", key);
            }
            result.setRowStream(holder.getStream());
        } catch (PhysicalException e) {
            logger.error("[LongQuery][AbstractFaultTolerancePolicy][key={}] persistence failure, because = {}", key, e);
        }
        logger.info("[LongQuery][AbstractFaultTolerancePolicy][key={}] persistence success", key);
    }
}
