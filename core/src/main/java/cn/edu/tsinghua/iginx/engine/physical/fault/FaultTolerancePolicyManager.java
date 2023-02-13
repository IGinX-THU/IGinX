package cn.edu.tsinghua.iginx.engine.physical.fault;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FaultTolerancePolicyManager {

    private static final Logger logger = LoggerFactory.getLogger(FaultTolerancePolicyManager.class);

    private static final FaultTolerancePolicyManager INSTANCE = new FaultTolerancePolicyManager();

    private FaultTolerancePolicyManager() {

    }

    public FaultTolerancePolicy getPolicy() {
        String policyName = ConfigDescriptor.getInstance().getConfig().getFaultTolerancePolicy();
        switch (policyName) {
            case "default":
                return DefaultFaultTolerancePolicy.getInstance();
            case "naive":
                return null;
            case "greedy":
                return GreedyFaultTolerancePolicy.getInstance();
            default:
                logger.warn("[LongQuery][FaultTolerancePolicyManager] unknown fault tolerance policy: {}", policyName);
                return DefaultFaultTolerancePolicy.getInstance();
        }
    }

    public static FaultTolerancePolicyManager getInstance() {
        return INSTANCE;
    }

}
