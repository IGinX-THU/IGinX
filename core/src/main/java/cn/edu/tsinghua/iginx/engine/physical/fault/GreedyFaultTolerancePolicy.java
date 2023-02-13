package cn.edu.tsinghua.iginx.engine.physical.fault;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.sharedstore.utils.RowStreamStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

public class GreedyFaultTolerancePolicy extends AbstractFaultTolerancePolicy {


    private static final Logger logger = LoggerFactory.getLogger(GreedyFaultTolerancePolicy.class);

    private static final double faultToleranceMaxCostRatio = ConfigDescriptor.getInstance().getConfig().getFaultToleranceMaxCost();

    private static final int faultToleranceMaxPersistSize = ConfigDescriptor.getInstance().getConfig().getFaultToleranceMaxPersistSize();

    private static final String TOTAL_SPAN = "total_span";

    private static final String SIZE = "size";

    private static final String ESTIMATED_PERSIST_TIME = "estimated_persist_time";

    private static final String ESTIMATED_LOAD_TIME = "estimated_load_time";

    private static final String REDO_COST = "redo_cost";

    private static final String REDO_COST_WITHOUT_PERSIST = "redo_cost_without_persist";

    private static final String TOTAL_PERSIST_TIME = "total_persist_time";

    private static final GreedyFaultTolerancePolicy instance = new GreedyFaultTolerancePolicy();

    private GreedyFaultTolerancePolicy() {

    }

    private boolean isLoad(PhysicalTask task) {
        return task.getOperators().get(0).getType() == OperatorType.Load;
    }

    @Override
    protected boolean needPersistence(PhysicalTask task, TaskExecuteResult result) {
        String key = RowStreamStoreUtils.encodeKey(task.getContext().getId(), task.getOperators().get(0).getSequence());

        // step1: 记录当前节点的总执行时间
        double totalSpan = task.getSpan();
        List<PhysicalTask> parentTasks = task.getParentTasks();
        for (PhysicalTask parentTask: parentTasks) {
            totalSpan += getExtraInfoAsDouble(parentTask, TOTAL_SPAN);
        }
        setExtraInfoAsDouble(task, TOTAL_SPAN, totalSpan);

        // step2: 预估当前算子输出数据量的总字节数
        long estimatedStreamSize = result.getEstimatedStreamSize();
        setExtraInfoAsLong(task, SIZE, estimatedStreamSize);

        // step3: 计算预期持计划 & 读取时间
        double estimatedPersistTime = RowStreamStoreUtils.estimatePersistAndSerializeTime(estimatedStreamSize);
        double estimatedLoadTime = RowStreamStoreUtils.estimateLoadAndDeserializeTime(estimatedStreamSize);
        setExtraInfoAsDouble(task, ESTIMATED_PERSIST_TIME, estimatedPersistTime);
        setExtraInfoAsDouble(task, ESTIMATED_LOAD_TIME, estimatedLoadTime);

        // step4: 计算不持久化情况下的重执行代价
        double redoCostWithoutPersist = task.getSpan();
        for (PhysicalTask parentTask: parentTasks) {
            redoCostWithoutPersist += getExtraInfoAsDouble(parentTask, REDO_COST);
        }
        setExtraInfoAsDouble(task, REDO_COST_WITHOUT_PERSIST, redoCostWithoutPersist);


        double redoCost = redoCostWithoutPersist;
        double totalPersistTime = 0.0;
        for (PhysicalTask parentTask: parentTasks) {
            totalPersistTime += getExtraInfoAsDouble(parentTask, TOTAL_PERSIST_TIME);
        }
        boolean needPersist = false;

        // step5: 输出数据量过大的算子不进行持久化
        if (estimatedStreamSize / 1024 / 1024 > faultToleranceMaxPersistSize) {
            logger.info("[LongQuery][GreedyFaultTolerancePolicy][key={}] estimated size[{}MB] is so big, may persist time is {}ms, so we skip persist.", key, estimatedStreamSize / 1024 / 1024, estimatedPersistTime);
        }
        // step6: 加载代价大于重计算代价，不进行持久化
        else if (redoCostWithoutPersist < estimatedLoadTime){
            logger.info("[LongQuery][GreedyFaultTolerancePolicy][key={}] estimated size is {}KB, estimated redo cost[without persist] is {}ms, redo cost[with persist] is {}ms, so we don't need to persist", key, estimatedStreamSize / 1024, redoCostWithoutPersist, estimatedLoadTime);
        }
        // step7: 加载代价小于重计算代价，可以选择性的持久化
        else {
            if (totalSpan * faultToleranceMaxCostRatio > totalPersistTime + estimatedPersistTime) {
                logger.info("[LongQuery][GreedyFaultTolerancePolicy][key={}] estimated size is {}KB, estimated redo cost[without persist] is {}ms, redo cost[with persist] is {}ms, and the persist cost of the subtree {}ms is smaller than max cost {}ms, so we need to persist"
                        , key, estimatedStreamSize / 1024, redoCostWithoutPersist, estimatedLoadTime, totalPersistTime + estimatedPersistTime, totalSpan * faultToleranceMaxCostRatio);
                needPersist = true;
            } else {
                logger.info("[LongQuery][GreedyFaultTolerancePolicy][key={}] estimated size is {}KB, estimated redo cost[without persist] is {}ms, redo cost[with persist] is {}ms, but the persist cost of the subtree {}ms is so bigger than max cost {}ms, so we don't need to persist"
                        , key, estimatedStreamSize / 1024, redoCostWithoutPersist, estimatedLoadTime, totalPersistTime + estimatedPersistTime, totalSpan * faultToleranceMaxCostRatio);
            }
        }

        if (needPersist) {
            redoCost = estimatedLoadTime;
            totalPersistTime += estimatedPersistTime;
        }

        setExtraInfoAsDouble(task, REDO_COST, redoCost);
        setExtraInfoAsDouble(task, TOTAL_PERSIST_TIME, totalPersistTime);
        return needPersist;
    }

    public static FaultTolerancePolicy getInstance() {
        return instance;
    }

    private double getExtraInfoAsDouble(PhysicalTask task, String key) {
        Object value = task.getExtraInfo(key);
        if (value == null) {
            return -1.0;
        }
        return (double) value;
    }

    private long getExtraInfoAsLong(PhysicalTask task, String key) {
        Object value = task.getExtraInfo(key);
        if (value == null) {
            return -1L;
        }
        return (long) value;
    }

    private void setExtraInfoAsDouble(PhysicalTask task, String key, double value) {
        task.setExtraInfo(key, value);
    }

    private void setExtraInfoAsLong(PhysicalTask task, String key, long value) {
        task.setExtraInfo(key, value);
    }

}
