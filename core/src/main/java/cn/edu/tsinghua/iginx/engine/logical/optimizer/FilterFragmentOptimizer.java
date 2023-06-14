package cn.edu.tsinghua.iginx.engine.logical.optimizer;

import static cn.edu.tsinghua.iginx.metadata.utils.FragmentUtils.keyFromColumnsIntervalToKeyInterval;

import cn.edu.tsinghua.iginx.engine.logical.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.logical.utils.OperatorUtils;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsRange;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.KeyInterval;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterFragmentOptimizer implements Optimizer {

    private static final IMetaManager metaManager = DefaultMetaManager.getInstance();

    private static final Logger logger = LoggerFactory.getLogger(FilterFragmentOptimizer.class);

    private static FilterFragmentOptimizer instance;

    private FilterFragmentOptimizer() {}

    public static FilterFragmentOptimizer getInstance() {
        if (instance == null) {
            synchronized (FilterFragmentOptimizer.class) {
                if (instance == null) {
                    instance = new FilterFragmentOptimizer();
                }
            }
        }
        return instance;
    }

    @Override
    public Operator optimize(Operator root) {
        // only optimize query
        if (root.getType() == OperatorType.CombineNonQuery
                || root.getType() == OperatorType.ShowTimeSeries) {
            return root;
        }

        List<Select> selectOperatorList = new ArrayList<>();
        OperatorUtils.findSelectOperators(selectOperatorList, root);

        if (selectOperatorList.isEmpty()) {
            logger.info("There is no filter in logical tree.");
            return root;
        }

        for (Select selectOperator : selectOperatorList) {
            filterFragmentByTimeRange(selectOperator);
        }
        return root;
    }

    private void filterFragmentByTimeRange(Select selectOperator) {
        List<String> pathList = OperatorUtils.findPathList(selectOperator);
        if (pathList.isEmpty()) {
            logger.error("Can not find paths in select operator.");
            return;
        }

        ColumnsRange interval =
                new ColumnsInterval(pathList.get(0), pathList.get(pathList.size() - 1));
        Map<ColumnsRange, List<FragmentMeta>> fragmentsByColumnsInterval =
                metaManager.getFragmentMapByColumnsRange(interval, true);
        Pair<Map<KeyInterval, List<FragmentMeta>>, List<FragmentMeta>> pair =
                keyFromColumnsIntervalToKeyInterval(fragmentsByColumnsInterval);
        Map<KeyInterval, List<FragmentMeta>> fragments = pair.k;
        List<FragmentMeta> dummyFragments = pair.v;

        Filter filter = selectOperator.getFilter();
        List<KeyRange> keyRanges = ExprUtils.getKeyRangesFromFilter(filter);

        List<Operator> unionList = new ArrayList<>();
        fragments.forEach(
                (k, v) -> {
                    List<Operator> joinList = new ArrayList<>();
                    v.forEach(
                            meta -> {
                                if (hasTimeRangeOverlap(meta, keyRanges)) {
                                    joinList.add(
                                            new Project(
                                                    new FragmentSource(meta),
                                                    pathList,
                                                    selectOperator.getTagFilter()));
                                }
                            });
                    Operator operator = OperatorUtils.joinOperatorsByTime(joinList);
                    if (operator != null) {
                        unionList.add(operator);
                    }
                });

        Operator root = OperatorUtils.unionOperators(unionList);
        if (!dummyFragments.isEmpty()) {
            List<Operator> joinList = new ArrayList<>();
            dummyFragments.forEach(
                    meta -> {
                        if (meta.isValid() && hasTimeRangeOverlap(meta, keyRanges)) {
                            joinList.add(
                                    new Project(
                                            new FragmentSource(meta),
                                            pathList,
                                            selectOperator.getTagFilter()));
                        }
                    });
            if (root != null) {
                joinList.add(root);
            }
            root = OperatorUtils.joinOperatorsByTime(joinList);
        }
        if (root != null) {
            selectOperator.setSource(new OperatorSource(root));
        }
    }

    private boolean hasTimeRangeOverlap(FragmentMeta meta, List<KeyRange> keyRanges) {
        KeyInterval interval = meta.getKeyInterval();
        for (KeyRange range : keyRanges) {
            if (interval.getStartKey() > range.getEndKey()
                    || interval.getEndKey() < range.getBeginKey()) {
                // continue
            } else {
                return true;
            }
        }
        return false;
    }
}
