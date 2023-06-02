package cn.edu.tsinghua.iginx.engine.logical.generator;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.data.write.ColumnDataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawData;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.CombineNonQuery;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.policy.PolicyManager;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InsertGenerator extends AbstractGenerator {

    private static final Logger logger = LoggerFactory.getLogger(InsertGenerator.class);
    private static final InsertGenerator instance = new InsertGenerator();
    private static final IMetaManager metaManager = DefaultMetaManager.getInstance();
    private final IPolicy policy =
            PolicyManager.getInstance()
                    .getPolicy(ConfigDescriptor.getInstance().getConfig().getPolicyClassName());

    private InsertGenerator() {
        this.type = GeneratorType.Insert;
    }

    public static InsertGenerator getInstance() {
        return instance;
    }

    @Override
    protected Operator generateRoot(Statement statement) {
        InsertStatement insertStatement = (InsertStatement) statement;

        policy.notify(insertStatement);

        List<String> pathList = new ArrayList<>(insertStatement.getPaths());

        ColumnsRange tsInterval =
                new ColumnsInterval(pathList.get(0), pathList.get(pathList.size() - 1));
        KeyInterval keyInterval =
                new KeyInterval(insertStatement.getStartTime(), insertStatement.getEndTime() + 1);

        Map<ColumnsRange, List<FragmentMeta>> fragments =
                metaManager.getFragmentMapByColumnsIntervalAndKeyInterval(
                        tsInterval, keyInterval);
        if (fragments.isEmpty()) {
            // on startup
            policy.setNeedReAllocate(false);
            Pair<List<FragmentMeta>, List<StorageUnitMeta>> fragmentsAndStorageUnits =
                    policy.generateInitialFragmentsAndStorageUnits(insertStatement);
            metaManager.createInitialFragmentsAndStorageUnits(
                    fragmentsAndStorageUnits.v, fragmentsAndStorageUnits.k);
            fragments = metaManager.getFragmentMapByColumnsRange(tsInterval);
        } else if (policy.isNeedReAllocate()) {
            // on scale-out or any events requiring reallocation
            logger.debug("Trig ReAllocate!");
            Pair<List<FragmentMeta>, List<StorageUnitMeta>> fragmentsAndStorageUnits =
                    policy.generateFragmentsAndStorageUnits(insertStatement);
            metaManager.createFragmentsAndStorageUnits(
                    fragmentsAndStorageUnits.v, fragmentsAndStorageUnits.k);
        }

        RawData rawData = insertStatement.getRawData();
        List<Insert> insertList = new ArrayList<>();
        fragments.forEach(
                (k, v) ->
                        v.forEach(
                                fragmentMeta -> {
                                    DataView section = getDataSection(fragmentMeta, rawData);
                                    if (section != null) {
                                        insertList.add(
                                                new Insert(
                                                        new FragmentSource(fragmentMeta), section));
                                    }
                                }));

        List<Source> sources = new ArrayList<>();
        insertList.forEach(operator -> sources.add(new OperatorSource(operator)));
        return new CombineNonQuery(sources);
    }

    private DataView getDataSection(FragmentMeta meta, RawData rawData) {
        KeyInterval keyInterval = meta.getKeyInterval();
        ColumnsRange tsInterval = meta.getColumnsRange();
        List<Long> insertTimes = rawData.getKeys();
        List<String> paths = rawData.getPaths();

        // time overlap doesn't exist.
        if (keyInterval.getStartKey() > insertTimes.get(insertTimes.size() - 1)
                || keyInterval.getEndKey() <= insertTimes.get(0)) {
            return null;
        }

        // path overlap doesn't exist.
        if (tsInterval.getStartColumn() != null
                && tsInterval.getStartColumn().compareTo(paths.get(paths.size() - 1)) > 0)
            return null;
        if (tsInterval.getEndColumn() != null
                && tsInterval.getEndColumn().compareTo(paths.get(0)) <= 0) {
            return null;
        }

        int startTimeIndex = 0;
        while (keyInterval.getStartKey() > insertTimes.get(startTimeIndex)) startTimeIndex++;
        int endTimeIndex = startTimeIndex;
        while (endTimeIndex < insertTimes.size()
                && keyInterval.getEndKey() > insertTimes.get(endTimeIndex)) endTimeIndex++;

        int startPathIndex = 0;
        if (tsInterval.getStartColumn() != null) {
            while (tsInterval.getStartColumn().compareTo(paths.get(startPathIndex)) > 0)
                startPathIndex++;
        }
        int endPathIndex = startPathIndex;
        if (tsInterval.getEndColumn() != null) {
            while (endPathIndex < paths.size()
                    && tsInterval.getEndColumn().compareTo(paths.get(endPathIndex)) > 0)
                endPathIndex++;
        } else {
            endPathIndex = paths.size();
        }

        if (rawData.isRowData()) {
            return new RowDataView(
                    rawData, startPathIndex, endPathIndex, startTimeIndex, endTimeIndex);
        } else {
            return new ColumnDataView(
                    rawData, startPathIndex, endPathIndex, startTimeIndex, endTimeIndex);
        }
    }
}
