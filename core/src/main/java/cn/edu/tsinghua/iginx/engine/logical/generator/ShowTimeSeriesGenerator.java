package cn.edu.tsinghua.iginx.engine.logical.generator;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.logical.utils.OperatorUtils;
import cn.edu.tsinghua.iginx.engine.logical.utils.PathUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.AddSchemaPrefix;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.ShowTimeSeries;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.GlobalSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.policy.PolicyManager;
import cn.edu.tsinghua.iginx.sql.statement.SelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.ShowTimeSeriesStatement;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static cn.edu.tsinghua.iginx.metadata.utils.FragmentUtils.keyFromTSIntervalToTimeInterval;

public class ShowTimeSeriesGenerator extends AbstractGenerator {

    private static final Logger logger = LoggerFactory.getLogger(ShowTimeSeriesGenerator.class);
    private final static ShowTimeSeriesGenerator instance = new ShowTimeSeriesGenerator();
    private final static IMetaManager metaManager = DefaultMetaManager.getInstance();
    private final IPolicy policy = PolicyManager.getInstance()
            .getPolicy(ConfigDescriptor.getInstance().getConfig().getPolicyClassName());

    private ShowTimeSeriesGenerator() {
        this.type = GeneratorType.ShowTimeSeries;
    }

    public static ShowTimeSeriesGenerator getInstance() {
        return instance;
    }

    private Pair<Map<TimeInterval, List<FragmentMeta>>, List<FragmentMeta>> getFragmentsByTSInterval(TimeSeriesInterval interval) {
        Map<TimeSeriesRange, List<FragmentMeta>> fragmentsByTSInterval = metaManager.getFragmentMapByTimeSeriesInterval(PathUtils.trimTimeSeriesInterval(interval), true);
//        if (!metaManager.hasFragment()) {
//            //on startup
//            Pair<List<FragmentMeta>, List<StorageUnitMeta>> fragmentsAndStorageUnits = policy.generateInitialFragmentsAndStorageUnits(selectStatement);
//            metaManager.createInitialFragmentsAndStorageUnits(fragmentsAndStorageUnits.v, fragmentsAndStorageUnits.k);
//            fragmentsByTSInterval = metaManager.getFragmentMapByTimeSeriesInterval(interval, true);
//        }
        return keyFromTSIntervalToTimeInterval(fragmentsByTSInterval);
    }

    @Override
    protected Operator generateRoot(Statement statement) {
        ShowTimeSeriesStatement showTimeSeriesStatement = (ShowTimeSeriesStatement) statement;
        return new ShowTimeSeries(
            new GlobalSource(),
            showTimeSeriesStatement.getPathRegexSet(),
            showTimeSeriesStatement.getTagFilter(),
            showTimeSeriesStatement.getLimit(),
            showTimeSeriesStatement.getOffset());
    }
}
