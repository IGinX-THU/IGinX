package cn.edu.tsinghua.iginx.engine.physical.fault;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.sharedstore.utils.RowStreamStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class DefaultFaultTolerancePolicy extends AbstractFaultTolerancePolicy {

    private static final Logger logger = LoggerFactory.getLogger(DefaultFaultTolerancePolicy.class);

    private static final Random random = new Random(0);

    private static final int faultToleranceMaxPersistSize = ConfigDescriptor.getInstance().getConfig().getFaultToleranceMaxPersistSize();

    private static final DefaultFaultTolerancePolicy instance = new DefaultFaultTolerancePolicy();

    private DefaultFaultTolerancePolicy() {

    }

    @Override
    protected boolean needPersistence(PhysicalTask task, TaskExecuteResult result) {
        String key = RowStreamStoreUtils.encodeKey(task.getContext().getId(), task.getOperators().get(0).getSequence());
        long estimatedStreamSize = result.getEstimatedStreamSize();
        double estimatedPersistTime = RowStreamStoreUtils.estimatePersistAndSerializeTime(estimatedStreamSize);
        if (result.getEstimatedStreamSize() > 1024L * 1024 * faultToleranceMaxPersistSize) {
            logger.info("[LongQuery][DefaultFaultTolerancePolicy][key={}] estimated size[{}MB] is so big, may persist time is {}ms, so we skip persist.", key, estimatedStreamSize / 1024 / 1024, estimatedPersistTime);
            return false;
        }
        if (task.getOperators().get(0).getType() == OperatorType.Load) {
            logger.info("[LongQuery][DefaultFaultTolerancePolicy][key={}] load operator means it has been persis, so we skip persist.", key);

        }
        boolean persist = random.nextInt(10) < 5;
        logger.info("[LongQuery][DefaultFaultTolerancePolicy][key={}] needPersist = {}, may persist time {}ms", key, persist, estimatedPersistTime);
        return persist;
    }

    public static FaultTolerancePolicy getInstance() {
        return instance;
    }

}
