package cn.edu.tsinghua.iginx.engine.logical.generator;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.logical.optimizer.LogicalOptimizerManager;
import cn.edu.tsinghua.iginx.engine.logical.utils.OperatorUtils;
import cn.edu.tsinghua.iginx.engine.logical.utils.PathUtils;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.manager.FunctionManager;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.FilterType;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.PathFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.FuncType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.policy.PolicyManager;
import cn.edu.tsinghua.iginx.sql.expression.Expression;
import cn.edu.tsinghua.iginx.sql.expression.Expression.ExpressionType;
import cn.edu.tsinghua.iginx.sql.statement.SelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.SelectStatement.QueryType;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import cn.edu.tsinghua.iginx.sql.statement.frompart.FromPartType;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;
import cn.edu.tsinghua.iginx.sql.statement.frompart.SubQueryFromPart;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.SortUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.*;
import static cn.edu.tsinghua.iginx.engine.shared.function.system.ArithmeticExpr.ARITHMETIC_EXPR;
import static cn.edu.tsinghua.iginx.metadata.utils.FragmentUtils.keyFromTSIntervalToTimeInterval;

public class QueryGenerator extends AbstractGenerator {

    private static final Logger logger = LoggerFactory.getLogger(QueryGenerator.class);
    private final static Config config = ConfigDescriptor.getInstance().getConfig();
    private final static QueryGenerator instance = new QueryGenerator();
    private final static FunctionManager functionManager = FunctionManager.getInstance();
    private final static IMetaManager metaManager = DefaultMetaManager.getInstance();
    private final IPolicy policy = PolicyManager.getInstance()
        .getPolicy(ConfigDescriptor.getInstance().getConfig().getPolicyClassName());

    private QueryGenerator() {
        this.type = GeneratorType.Query;
        LogicalOptimizerManager optimizerManager = LogicalOptimizerManager.getInstance();
        String[] optimizers = config.getQueryOptimizer().split(",");
        for (String optimizer : optimizers) {
            registerOptimizer(optimizerManager.getOptimizer(optimizer));
        }
    }

    public static QueryGenerator getInstance() {
        return instance;
    }

    @Override
    protected Operator generateRoot(Statement statement) {
        SelectStatement selectStatement = (SelectStatement) statement;

        Operator root;
        if (selectStatement.hasJoinParts()) {
            root = filterAndMergeFragmentsWithJoin(selectStatement);
        } else {
            if (selectStatement.getFromParts().isEmpty()) {
                policy.notify(selectStatement);
                root = filterAndMergeFragments(selectStatement);
            } else {
                if (selectStatement.getFromParts().get(0).getType() == FromPartType.SubQueryFromPart) {
                    SubQueryFromPart fromPart = (SubQueryFromPart) selectStatement.getFromParts().get(0);
                    root = generateRoot(fromPart.getSubQuery());
                } else {
                    policy.notify(selectStatement);
                    root = filterAndMergeFragments(selectStatement);
                }
            }
        }

        TagFilter tagFilter = selectStatement.getTagFilter();

        if (selectStatement.hasValueFilter()) {
            root = new Select(new OperatorSource(root), selectStatement.getFilter(), tagFilter);
        }

        List<Operator> queryList = new ArrayList<>();
        if (selectStatement.getQueryType() == QueryType.GroupByQuery) {
            // Downsample Query
            List<FunctionCall> functionCallList = new ArrayList<>();
            selectStatement.getBaseExpressionMap().forEach((k, v) -> {
                if (!k.equals("")) {
                    v.forEach(expression -> {
                        Map<String, Value> params = new HashMap<String, Value>(){{
                            put(PARAM_PATHS, new Value(expression.getPathName()));
                        }};
                        functionCallList.add(new FunctionCall(functionManager.getFunction(k), params));
                    });
                }
            });
            queryList.add(new GroupBy(new OperatorSource(root), selectStatement.getGroupByPaths(), functionCallList));
        } else if (selectStatement.getQueryType() == SelectStatement.QueryType.DownSampleQuery) {
            // DownSample Query
            Operator finalRoot = root;
            selectStatement.getBaseExpressionMap().forEach((k, v) -> v.forEach(expression -> {
                Map<String, Value> params = new HashMap<>();
                params.put(PARAM_PATHS, new Value(expression.getPathName()));
                if (!selectStatement.getLayers().isEmpty()) {
                    params.put(PARAM_LEVELS, new Value(selectStatement.getLayers().stream().map(String::valueOf).collect(Collectors.joining(","))));
                }
                Operator copySelect = finalRoot.copy();
                queryList.add(
                    new Downsample(
                        new OperatorSource(copySelect),
                        selectStatement.getPrecision(),
                        selectStatement.getSlideDistance(),
                        new FunctionCall(functionManager.getFunction(k), params),
                        new TimeRange(selectStatement.getStartTime(), selectStatement.getEndTime())
                    )
                );
            }));
        } else if (selectStatement.getQueryType() == SelectStatement.QueryType.AggregateQuery) {
            // Aggregate Query
            Operator finalRoot = root;
            selectStatement.getBaseExpressionMap().forEach((k, v) -> v.forEach(expression -> {
                Map<String, Value> params = new HashMap<>();
                params.put(PARAM_PATHS, new Value(expression.getPathName()));
                if (!selectStatement.getLayers().isEmpty()) {
                    params.put(PARAM_LEVELS, new Value(selectStatement.getLayers().stream().map(String::valueOf).collect(Collectors.joining(","))));
                }
                Operator copySelect = finalRoot.copy();
                if (k.equals("")) {
                    queryList.add(new Project(new OperatorSource(copySelect),
                        Collections.singletonList(expression.getPathName()), tagFilter));
                } else {
                    logger.info("function: " + k + ", wrapped path: " + expression.getPathName());
                    if (FunctionUtils.isRowToRowFunction(k)) {
                        queryList.add(
                            new RowTransform(
                                new OperatorSource(copySelect),
                                new FunctionCall(functionManager.getFunction(k), params)
                            )
                        );
                    } else if (FunctionUtils.isSetToSetFunction(k)) {
                        queryList.add(
                            new MappingTransform(
                                new OperatorSource(copySelect),
                                new FunctionCall(functionManager.getFunction(k), params)
                            )
                        );
                    } else {
                        queryList.add(
                            new SetTransform(
                                new OperatorSource(copySelect),
                                new FunctionCall(functionManager.getFunction(k), params)
                            )
                        );
                    }
                }
            }));
        } else if (selectStatement.getQueryType() == SelectStatement.QueryType.LastFirstQuery) {
            Operator finalRoot = root;
            selectStatement.getBaseExpressionMap().forEach((k, v) -> v.forEach(expression -> {
                Map<String, Value> params = new HashMap<>();
                params.put(PARAM_PATHS, new Value(expression.getPathName()));
                Operator copySelect = finalRoot.copy();
                logger.info("function: " + k + ", wrapped path: " + v);
                queryList.add(
                    new MappingTransform(
                        new OperatorSource(copySelect),
                        new FunctionCall(functionManager.getFunction(k), params)
                    )
                );
            }));
        } else {
            List<String> selectedPath = new ArrayList<>();
            selectStatement.getBaseExpressionMap().forEach((k, v) ->
                v.forEach(expression -> selectedPath.add(expression.getPathName())));
            queryList.add(new Project(new OperatorSource(root), selectedPath, tagFilter));
        }

        if (selectStatement.getQueryType() == SelectStatement.QueryType.LastFirstQuery) {
            root = OperatorUtils.unionOperators(queryList);
        } else if (selectStatement.getQueryType() == SelectStatement.QueryType.DownSampleQuery) {
            root = OperatorUtils.joinOperatorsByTime(queryList);
        } else {
            if (selectStatement.getFuncTypeSet().contains(FuncType.Udtf)) {
                root = OperatorUtils.joinOperatorsByTime(queryList);
            } else {
                root = OperatorUtils.joinOperators(queryList, ORDINAL);
            }
        }

        if (selectStatement.getHavingFilter() != null) {
            root = new Select(new OperatorSource(root), selectStatement.getHavingFilter(), null);
        }

        List<Operator> exprList = new ArrayList<>();
        exprList.add(root);
        for (Expression expression : selectStatement.getExpressions()) {
            if (!expression.getType().equals(ExpressionType.Base)) {
                Operator copySelect = root.copy();
                Map<String, Value> params = new HashMap<>();
                params.put(PARAM_EXPR, new Value(expression));

                exprList.add(
                    new RowTransform(
                        new OperatorSource(copySelect),
                        new FunctionCall(functionManager.getFunction(ARITHMETIC_EXPR), params)
                    )
                );
            }
        }
        root = OperatorUtils.joinOperatorsByTime(exprList);

        if (!selectStatement.getOrderByPaths().isEmpty()) {
            root = new Sort(
                new OperatorSource(root),
                selectStatement.getOrderByPaths(),
                selectStatement.isAscending() ? Sort.SortType.ASC : Sort.SortType.DESC
            );
        }

        if (selectStatement.getLimit() != Integer.MAX_VALUE || selectStatement.getOffset() != 0) {
            root = new Limit(
                new OperatorSource(root),
                (int) selectStatement.getLimit(),
                (int) selectStatement.getOffset()
            );
        }

//        if (selectStatement.getQueryType().equals(SelectStatement.QueryType.LastFirstQuery)) {
//            root = new Reorder(new OperatorSource(root), Arrays.asList("path", "value"));
//        } else {
//            List<String> order = new ArrayList<>();
//            selectStatement.getExpressions().forEach(expression -> {
//                String colName = expression.getColumnName();
//                order.add(colName);
//            });
//            root = new Reorder(new OperatorSource(root), order);
//        }

        Map<String, String> aliasMap = selectStatement.getAliasMap();
        if (!aliasMap.isEmpty()) {
            root = new Rename(new OperatorSource(root), aliasMap);
        }

        return root;
    }

    private Operator filterAndMergeFragments(SelectStatement selectStatement) {
        List<String> pathList = SortUtils.mergeAndSortPaths(new ArrayList<>(selectStatement.getPathSet()));
        TagFilter tagFilter = selectStatement.getTagFilter();

        TimeSeriesInterval interval = new TimeSeriesInterval(pathList.get(0), pathList.get(pathList.size() - 1));

        Pair<Map<TimeInterval, List<FragmentMeta>>, List<FragmentMeta>> pair = getFragmentsByTSInterval(selectStatement, interval);
        Map<TimeInterval, List<FragmentMeta>> fragments = pair.k;
        List<FragmentMeta> dummyFragments = pair.v;

        return mergeRawData(fragments, dummyFragments, pathList, tagFilter);
    }

    private Operator filterAndMergeFragmentsWithJoin(SelectStatement selectStatement) {
        TagFilter tagFilter = selectStatement.getTagFilter();

        List<Operator> joinList = new ArrayList<>();
        // 1. get all data of single prefix like a.* or b.*
        selectStatement.getFromParts().forEach(fromPart -> {
            if(fromPart.getType() == FromPartType.SubQueryFromPart) {
                SubQueryFromPart subQueryFromPart = (SubQueryFromPart) fromPart;
                joinList.add(generateRoot(subQueryFromPart.getSubQuery()));
            } else {
                String prefix = fromPart.getPath() + ALL_PATH_SUFFIX;
                Pair<Map<TimeInterval, List<FragmentMeta>>, List<FragmentMeta>> pair = getFragmentsByTSInterval(selectStatement, new TimeSeriesInterval(prefix, prefix));
                Map<TimeInterval, List<FragmentMeta>> fragments = pair.k;
                List<FragmentMeta> dummyFragments = pair.v;
                joinList.add(mergeRawData(fragments, dummyFragments, Collections.singletonList(prefix), tagFilter));
            }
        });
        // 2. merge by declare
        Operator left = joinList.get(0);
        String prefixA = selectStatement.getGlobalAlias();
        for (int i = 1; i < joinList.size(); i++) {
            JoinCondition joinCondition = selectStatement.getFromParts().get(i).getJoinCondition();
            Operator right = joinList.get(i);

            String prefixB = selectStatement.getFromParts().get(i).getPath();

            JoinAlgType joinAlgType = JoinAlgType.NestedLoopJoin;
            Filter filter = joinCondition.getFilter();
            if (filter != null && filter.getType().equals(FilterType.Path)) {
                PathFilter pathFilter = (PathFilter) filter;
                if (pathFilter.getOp() == Op.E) {
                    joinAlgType = JoinAlgType.HashJoin;
                }
            }

            List<String> joinColumns = joinCondition.getJoinColumns();
            if (joinColumns != null && joinColumns.size() == 1) {
                joinAlgType = JoinAlgType.HashJoin;
            }

            switch (joinCondition.getJoinType()) {
                case CrossJoin:
                    left = new CrossJoin(new OperatorSource(left), new OperatorSource(right), prefixA, prefixB);
                    break;
                case InnerJoin:
                    left = new InnerJoin(new OperatorSource(left), new OperatorSource(right), prefixA, prefixB, filter, joinColumns, false, joinAlgType);
                    break;
                case InnerNatualJoin:
                    left = new InnerJoin(new OperatorSource(left), new OperatorSource(right), prefixA, prefixB, filter, joinColumns, true, joinAlgType);
                    break;
                case LeftNatualJoin:
                    left = new OuterJoin(new OperatorSource(left), new OperatorSource(right), prefixA, prefixB, OuterJoinType.LEFT, filter, joinColumns, true, joinAlgType);
                    break;
                case RightNatualJoin:
                    new OuterJoin(new OperatorSource(left), new OperatorSource(right), prefixA, prefixB, OuterJoinType.RIGHT, filter, joinColumns, true, joinAlgType);
                    break;
                case FullOuterJoin:
                    left = new OuterJoin(new OperatorSource(left), new OperatorSource(right), prefixA, prefixB, OuterJoinType.FULL, filter, joinColumns, false, joinAlgType);
                    break;
                case LeftOuterJoin:
                    left = new OuterJoin(new OperatorSource(left), new OperatorSource(right), prefixA, prefixB, OuterJoinType.LEFT, filter, joinColumns, false, joinAlgType);
                    break;
                case RightOuterJoin:
                    left = new OuterJoin(new OperatorSource(left), new OperatorSource(right), prefixA, prefixB, OuterJoinType.RIGHT, filter, joinColumns, false, joinAlgType);
                    break;
            }

            prefixA = prefixB;
        }
        return left;
    }

    private Operator mergeRawData(Map<TimeInterval, List<FragmentMeta>> fragments, List<FragmentMeta> dummyFragments, List<String> pathList, TagFilter tagFilter) {
        List<Operator> unionList = new ArrayList<>();
        fragments.forEach((k, v) -> {
            List<Operator> joinList = new ArrayList<>();
            v.forEach(meta -> joinList.add(new Project(new FragmentSource(meta), pathList, tagFilter)));
            unionList.add(OperatorUtils.joinOperatorsByTime(joinList));
        });

        Operator operator = OperatorUtils.unionOperators(unionList);
        if (!dummyFragments.isEmpty()) {
            List<Operator> joinList = new ArrayList<>();
            dummyFragments.forEach(meta -> {
                if (meta.isValid()) {
                    String schemaPrefix = meta.getTsInterval().getSchemaPrefix();
                    joinList.add(new AddSchemaPrefix(new OperatorSource(new Project(new FragmentSource(meta),
                        pathMatchPrefix(pathList, meta.getTsInterval().getTimeSeries(), schemaPrefix), tagFilter)), schemaPrefix));
                }
            });
            joinList.add(operator);
            operator = OperatorUtils.joinOperatorsByTime(joinList);
        }
        return operator;
    }

    private Pair<Map<TimeInterval, List<FragmentMeta>>, List<FragmentMeta>> getFragmentsByTSInterval(SelectStatement selectStatement, TimeSeriesInterval interval) {
        Map<TimeSeriesRange, List<FragmentMeta>> fragmentsByTSInterval = metaManager.getFragmentMapByTimeSeriesInterval(PathUtils.trimTimeSeriesInterval(interval), true);
        if (!metaManager.hasFragment()) {
            //on startup
            Pair<List<FragmentMeta>, List<StorageUnitMeta>> fragmentsAndStorageUnits = policy.generateInitialFragmentsAndStorageUnits(selectStatement);
            metaManager.createInitialFragmentsAndStorageUnits(fragmentsAndStorageUnits.v, fragmentsAndStorageUnits.k);
            fragmentsByTSInterval = metaManager.getFragmentMapByTimeSeriesInterval(interval, true);
        }
        return keyFromTSIntervalToTimeInterval(fragmentsByTSInterval);
    }

    // 筛选出满足 dataPrefix前缀，并且去除 schemaPrefix
    private List<String> pathMatchPrefix(List<String> pathList, String prefix, String schemaPrefix) {
        if (prefix == null && schemaPrefix == null) return pathList;
        List<String> ans = new ArrayList<>();

        if (prefix == null) { // deal with the schemaPrefix
            for(String path : pathList) {
                if (path.equals("*.*") || path.equals("*")) {
                    ans.add(path);
                } else if (path.indexOf(schemaPrefix) == 0) {
                    path = path.substring(schemaPrefix.length() + 1);
                    ans.add(path);
                }
            }
            return ans;
        }
//        if (schemaPrefix != null) prefix = schemaPrefix + "." + prefix;

        for(String path : pathList) {
            if (schemaPrefix != null && path.indexOf(schemaPrefix) == 0) {
                path = path.substring(schemaPrefix.length() + 1);
            }
            if (path.equals("*.*") || path.equals("*")) {
                ans.add(prefix + ".*");
            } else if (path.charAt(path.length()-1) == '*' && path.length() != 1) { // 通配符匹配，例如 a.b.*
                String queryPrefix = path.substring(0,path.length()-2) + ".(.*)";
                if (prefix.matches(queryPrefix)) {
                    ans.add(path);
                    continue;
                }
                queryPrefix = prefix + ".(.*)";
                if (path.matches(queryPrefix)) {
                    ans.add(path);
                }
            } else if (!path.contains("*")) { // 例如 a.b.f 这样确切的路径信息
                String queryPrefix = prefix + ".(.*)";
                if (path.matches(queryPrefix)) {
                    ans.add(path);
                }
            }
        }
        return ans;
    }
}
