package cn.edu.tsinghua.iginx.engine.physical.fault;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.sharedstore.utils.RowStreamStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class NaiveFaultTolerancePolicy extends AbstractFaultTolerancePolicy {

    private static final Logger logger = LoggerFactory.getLogger(NaiveFaultTolerancePolicy.class);

    private static final double faultToleranceMaxCostRatio = ConfigDescriptor.getInstance().getConfig().getFaultToleranceMaxCost();

    private static final int faultToleranceMaxPersistSize = ConfigDescriptor.getInstance().getConfig().getFaultToleranceMaxPersistSize();

    private static final NaiveFaultTolerancePolicy instance = new NaiveFaultTolerancePolicy();

    private NaiveFaultTolerancePolicy() {

    }

    @Override
    protected boolean needPersistence(PhysicalTask task, TaskExecuteResult result) {
        String key = RowStreamStoreUtils.encodeKey(task.getContext().getId(), task.getOperators().get(0).getSequence());
        long estimatedStreamSize = result.getEstimatedStreamSize();
        double estimatedPersistTime = RowStreamStoreUtils.estimatePersistAndSerializeTime(estimatedStreamSize);
        if (result.getEstimatedStreamSize() > 1024L * 1024 * faultToleranceMaxPersistSize) {
            logger.info("[LongQuery][NaiveFaultTolerancePolicy][key={}] estimated size[{}MB] is so big, may persist time is {}ms, so we skip persist.", key, estimatedStreamSize / 1024 / 1024, estimatedPersistTime);
            return false;
        }
        if (task.getOperators().get(0).getType() == OperatorType.Load) {
            logger.info("[LongQuery][NaiveFaultTolerancePolicy][key={}] load operator means it has been persis, so we skip persist.", key);
            return false;
        }
        long span = task.getSpan();
        boolean persist = span * faultToleranceMaxCostRatio > estimatedPersistTime;
        logger.info("[LongQuery][DefaultFaultTolerancePolicy][key={}] needPersist = {}, may persist time {}ms", key, persist, estimatedPersistTime);
        return persist;
    }

    public static FaultTolerancePolicy getInstance() {
        return instance;
    }


}
