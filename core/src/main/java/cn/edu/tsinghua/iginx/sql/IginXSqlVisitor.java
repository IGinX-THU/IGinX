package cn.edu.tsinghua.iginx.sql;

import cn.edu.tsinghua.iginx.engine.logical.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.FuncType;
import cn.edu.tsinghua.iginx.exceptions.SQLParserException;
import cn.edu.tsinghua.iginx.sql.SqlParser.*;
import cn.edu.tsinghua.iginx.sql.expression.*;
import cn.edu.tsinghua.iginx.sql.expression.Expression.ExpressionType;
import cn.edu.tsinghua.iginx.sql.statement.*;
import cn.edu.tsinghua.iginx.sql.statement.frompart.*;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinType;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.TimeUtils;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.*;

import static cn.edu.tsinghua.iginx.sql.statement.SelectStatement.markJoinCount;

public class IginXSqlVisitor extends SqlBaseVisitor<Statement> {

    private final static Set<FuncType> supportedAggregateWithLevelFuncSet = new HashSet<>(
        Arrays.asList(
            FuncType.Sum,
            FuncType.Count,
            FuncType.Avg
        )
    );



    @Override
    public Statement visitSqlStatement(SqlStatementContext ctx) {
        return visit(ctx.statement());
    }

    @Override
    public Statement visitInsertStatement(InsertStatementContext ctx) {
        boolean hasSubQuery = ctx.insertValuesSpec().queryClause() != null;

        InsertStatement insertStatement;
        if (hasSubQuery) {
            insertStatement = new InsertStatement(RawDataType.NonAlignedRow);
        } else {
            insertStatement = new InsertStatement(RawDataType.NonAlignedColumn);
        }

        insertStatement.setPrefixPath(ctx.path().getText());

        if (ctx.tagList() != null) {
            Map<String, String> globalTags = parseTagList(ctx.tagList());
            insertStatement.setGlobalTags(globalTags);
        }
        // parse paths
        Set<Pair<String, Map<String, String>>> columnsSet = new HashSet<>();
        ctx.insertColumnsSpec().insertPath().forEach(e -> {
            String path = e.path().getText();
            Map<String, String> tags;
            if (e.tagList() != null) {
                if (insertStatement.hasGlobalTags()) {
                    throw new SQLParserException(
                        "Insert path couldn't has global tags and local tags at the same time.");
                }
                tags = parseTagList(e.tagList());
            } else {
                tags = insertStatement.getGlobalTags();
            }
            if (!columnsSet.add(new Pair<>(path, tags))) {
                throw new SQLParserException(
                    "Insert statements should not contain duplicate paths.");
            }
            insertStatement.setPath(path, tags);
        });

        InsertValuesSpecContext valuesSpecContext = ctx.insertValuesSpec();
        if (hasSubQuery) {
            SelectStatement selectStatement = new SelectStatement();
            parseQueryClause(ctx.insertValuesSpec().queryClause(), selectStatement);
            long timeOffset = valuesSpecContext.TIME_OFFSET() == null ? 0
                : Long.parseLong(valuesSpecContext.INT().getText());
            return new InsertFromSelectStatement(timeOffset, selectStatement, insertStatement);
        } else {
            // parse times, values and types
            parseInsertValuesSpec(valuesSpecContext, insertStatement);
            if (insertStatement.getPaths().size() != insertStatement.getValues().length) {
                throw new SQLParserException("Insert path size and value size must be equal.");
            }
            insertStatement.sortData();
            return insertStatement;
        }
    }

    @Override
    public Statement visitDeleteStatement(DeleteStatementContext ctx) {
        DeleteStatement deleteStatement = new DeleteStatement();
        // parse delete paths
        ctx.path().forEach(e -> deleteStatement.addPath(e.getText()));
        // parse time range
        if (ctx.whereClause() != null) {
            Filter filter = parseOrExpression(ctx.whereClause().orExpression(), deleteStatement);
            deleteStatement.setTimeRangesByFilter(filter);
        } else {
            List<TimeRange> timeRanges = new ArrayList<>(
                Collections.singletonList(new TimeRange(0, Long.MAX_VALUE)));
            deleteStatement.setTimeRanges(timeRanges);
        }
        // parse tag filter
        if (ctx.withClause() != null) {
            TagFilter tagFilter = parseWithClause(ctx.withClause());
            deleteStatement.setTagFilter(tagFilter);
        }
        return deleteStatement;
    }

    @Override
    public Statement visitSelectStatement(SelectStatementContext ctx) {
        SelectStatement selectStatement = new SelectStatement();
        if (ctx.EXPLAIN() != null) {
            if (ctx.PHYSICAL() != null) {
                selectStatement.setNeedPhysicalExplain(true);
            } else {
                selectStatement.setNeedLogicalExplain(true);
            }
        }
        if (ctx.queryClause() != null) {
            parseQueryClause(ctx.queryClause(), selectStatement);
        }
        return selectStatement;
    }

    private void parseQueryClause(QueryClauseContext ctx, SelectStatement selectStatement) {
        // Step 1. parse as much information as possible.
        // parse from paths
        if (ctx.fromClause() != null) {
            parseFromPaths(ctx.fromClause(), selectStatement);
        }
        // parse select paths
        if (ctx.selectClause() != null) {
            parseSelectPaths(ctx.selectClause(), selectStatement);
        }
        // parse where clause
        if (ctx.whereClause() != null) {
            Filter filter = parseOrExpression(ctx.whereClause().orExpression(), selectStatement);
            filter = ExprUtils.removeSingleFilter(filter);
            selectStatement.setFilter(filter);
            selectStatement.setHasValueFilter(true);
        }
        // parse with clause
        if (ctx.withClause() != null) {
            TagFilter tagFilter = parseWithClause(ctx.withClause());
            selectStatement.setTagFilter(tagFilter);
        }
        // parse special clause
        if (ctx.specialClause() != null) {
            parseSpecialClause(ctx.specialClause(), selectStatement);
        }
        // parse as clause
        if (ctx.asClause() != null) {
            parseAsClause(ctx.asClause(), selectStatement);
        }

        // Step 2. decide the query type according to the information.
        selectStatement.checkQueryType();
    }

    @Override
    public Statement visitDeleteTimeSeriesStatement(DeleteTimeSeriesStatementContext ctx) {
        DeleteTimeSeriesStatement deleteTimeSeriesStatement = new DeleteTimeSeriesStatement();
        ctx.path().forEach(e -> deleteTimeSeriesStatement.addPath(e.getText()));

        if (ctx.withClause() != null) {
            TagFilter tagFilter = parseWithClause(ctx.withClause());
            deleteTimeSeriesStatement.setTagFilter(tagFilter);
        }
        return deleteTimeSeriesStatement;
    }

    @Override
    public Statement visitCountPointsStatement(CountPointsStatementContext ctx) {
        return new CountPointsStatement();
    }

    @Override
    public Statement visitClearDataStatement(ClearDataStatementContext ctx) {
        return new ClearDataStatement();
    }

    @Override
    public Statement visitShowReplicationStatement(ShowReplicationStatementContext ctx) {
        return new ShowReplicationStatement();
    }

    @Override
    public Statement visitAddStorageEngineStatement(AddStorageEngineStatementContext ctx) {
        AddStorageEngineStatement addStorageEngineStatement = new AddStorageEngineStatement();
        // parse engines
        List<StorageEngineContext> engines = ctx.storageEngineSpec().storageEngine();
        for (StorageEngineContext engine : engines) {
            String ipStr = engine.ip.getText();
            String ip = ipStr.substring(ipStr.indexOf(SQLConstant.QUOTE) + 1,
                ipStr.lastIndexOf(SQLConstant.QUOTE));
            int port = Integer.parseInt(engine.port.getText());
            String typeStr = engine.engineType.getText().trim();
            String type = typeStr.substring(typeStr.indexOf(SQLConstant.QUOTE) + 1,
                typeStr.lastIndexOf(SQLConstant.QUOTE));
            Map<String, String> extra = parseExtra(engine.extra);
            addStorageEngineStatement.setEngines(new StorageEngine(ip, port, type, extra));
        }
        return addStorageEngineStatement;
    }

    @Override
    public Statement visitShowTimeSeriesStatement(ShowTimeSeriesStatementContext ctx) {
        ShowTimeSeriesStatement showTimeSeriesStatement = new ShowTimeSeriesStatement();
        for (PathContext pathRegex : ctx.path()) {
            showTimeSeriesStatement.setPathRegex(pathRegex.getText());
        }
        if (ctx.withClause() != null) {
            TagFilter tagFilter = parseWithClause(ctx.withClause());
            showTimeSeriesStatement.setTagFilter(tagFilter);
        }
        if (ctx.limitClause() != null) {
            Pair<Integer, Integer> limitAndOffset = parseLimitClause(ctx.limitClause());
            showTimeSeriesStatement.setLimit(limitAndOffset.getK());
            showTimeSeriesStatement.setOffset(limitAndOffset.getV());
        }
        return showTimeSeriesStatement;
    }

    @Override
    public Statement visitShowClusterInfoStatement(ShowClusterInfoStatementContext ctx) {
        return new ShowClusterInfoStatement();
    }

    @Override
    public Statement visitRemoveHistoryDataResourceStatement(
        RemoveHistoryDataResourceStatementContext ctx) {
        RemoveHsitoryDataSourceStatement statement = new RemoveHsitoryDataSourceStatement();
        ctx.removedStorageEngine().forEach(storageEngine -> {
            String ipStr = storageEngine.ip.getText();
            String schemaPrefixStr = storageEngine.schemaPrefix.getText();
            String dataPrefixStr = storageEngine.dataPrefix.getText();
            String ip = ipStr.substring(ipStr.indexOf(SQLConstant.QUOTE) + 1,
                ipStr.lastIndexOf(SQLConstant.QUOTE));
            String schemaPrefix = schemaPrefixStr
                .substring(schemaPrefixStr.indexOf(SQLConstant.QUOTE) + 1,
                    schemaPrefixStr.lastIndexOf(SQLConstant.QUOTE));
            String dataPrefix = dataPrefixStr
                .substring(dataPrefixStr.indexOf(SQLConstant.QUOTE) + 1,
                    dataPrefixStr.lastIndexOf(SQLConstant.QUOTE));
            statement.addStorageEngine(
                new RemovedStorageEngineInfo(ip, Integer.parseInt(storageEngine.port.getText()),
                    schemaPrefix, dataPrefix));
        });
        return statement;
    }

    private void parseFromPaths(FromClauseContext ctx, SelectStatement selectStatement) {
        List<FromPart> fromParts = new ArrayList<>();
        if (ctx.tableReference().path() != null) {
            String fromPath = ctx.tableReference().path().getText();
            fromParts.add(new PathFromPart(fromPath));
            selectStatement.setGlobalAlias(fromPath);
        } else {
            SelectStatement subStatement = new SelectStatement();
            subStatement.setIsSubQuery(true);
            parseQueryClause(ctx.tableReference().subquery().queryClause(), subStatement);
            selectStatement.setGlobalAlias(subStatement.getGlobalAlias());
            fromParts.add(new SubQueryFromPart(subStatement));
        }

        if (ctx.joinPart() != null && !ctx.joinPart().isEmpty()) {
            selectStatement.setHasJoinParts(true);

            // 当FROM子句有多个部分时，如果某一部分是子查询，该子查询必须使用AS子句
            if (fromParts.get(0).getType() == FromPartType.SubQueryFromPart) {
                SubQueryFromPart subQueryFromPart = (SubQueryFromPart) fromParts.get(0);
                if (subQueryFromPart.getSubQuery().hasJoinParts() && ctx.tableReference().subquery().queryClause().asClause() == null) {
                    throw new SQLParserException("AS clause is required in this sub query");
                }
            }
            
            for (JoinPartContext joinPartContext : ctx.joinPart()) {
                String pathPrefix;
                SelectStatement subStatement = new SelectStatement();
                if (joinPartContext.tableReference().path() != null) {
                    pathPrefix = joinPartContext.tableReference().path().getText();
                    subStatement = null;
                } else {
                    subStatement.setIsSubQuery(true);
                    parseQueryClause(joinPartContext.tableReference().subquery().queryClause(), subStatement);
                    // 当FROM子句有多个部分时，如果某一部分是子查询，该子查询必须使用AS子句
                    if (subStatement.hasJoinParts() && joinPartContext.tableReference().subquery().queryClause().asClause() == null) {
                        throw new SQLParserException("AS clause is required in this sub query");
                    }
                    pathPrefix = subStatement.getGlobalAlias();
                }
                if (joinPartContext.join() == null) {  // cross join
                    if (subStatement == null) {
                        fromParts.add(new PathFromPart(pathPrefix, new JoinCondition()));
                    } else {
                        // TODO: check correlated
                        fromParts.add(new SubQueryFromPart(subStatement, new JoinCondition()));
                    }
                    continue;
                }

                JoinType joinType = parseJoinType(joinPartContext.join());

                Filter filter = null;
                if (joinPartContext.orExpression() != null) {
                    filter = parseOrExpression(joinPartContext.orExpression(), selectStatement);
                }

                List<String> columns = new ArrayList<>();
                if (joinPartContext.colList() != null && !joinPartContext.colList().isEmpty()) {
                    joinPartContext.colList().path().forEach(pathContext -> columns.add(pathContext.getText()));
                }

                if (subStatement == null) {
                    fromParts.add(new PathFromPart(pathPrefix, new JoinCondition(joinType, filter, columns)));
                } else {
                    fromParts.add(new SubQueryFromPart(subStatement, new JoinCondition(joinType, filter, columns)));
                }
            }
        }
        selectStatement.setFromParts(fromParts);
    }

    private JoinType parseJoinType(JoinContext joinContext) {
        if (joinContext.NATURAL() != null) {
            if (joinContext.LEFT() != null) {
                return JoinType.LeftNaturalJoin;
            } else if (joinContext.RIGHT() != null) {
                return JoinType.RightNaturalJoin;
            } else {
                return JoinType.InnerNaturalJoin;
            }
        } else if (joinContext.LEFT() != null) {
            return JoinType.LeftOuterJoin;
        } else if (joinContext.RIGHT() != null) {
            return JoinType.RightOuterJoin;
        } else if (joinContext.FULL() != null) {
            return JoinType.FullOuterJoin;
        } else {
            return JoinType.InnerJoin;
        }
    }

    @Override
    public Statement visitShowRegisterTaskStatement(ShowRegisterTaskStatementContext ctx) {
        return new ShowRegisterTaskStatement();
    }

    @Override
    public Statement visitRegisterTaskStatement(RegisterTaskStatementContext ctx) {
        String filePath = ctx.filePath.getText();
        filePath = filePath.substring(1, filePath.length() - 1);

        String className = ctx.className.getText();
        className = className.substring(1, className.length() - 1);

        String name = ctx.name.getText();
        name = name.substring(1, name.length() - 1);

        UDFType type = UDFType.TRANSFORM;
        if (ctx.udfType().UDTF() != null) {
            type = UDFType.UDTF;
        } else if (ctx.udfType().UDAF() != null) {
            type = UDFType.UDAF;
        } else if (ctx.udfType().UDSF() != null) {
            type = UDFType.UDSF;
        }
        return new RegisterTaskStatement(name, filePath, className, type);
    }

    @Override
    public Statement visitDropTaskStatement(DropTaskStatementContext ctx) {
        String name = ctx.name.getText();
        name = name.substring(1, name.length() - 1);
        return new DropTaskStatement(name);
    }

    @Override
    public Statement visitCommitTransformJobStatement(CommitTransformJobStatementContext ctx) {
        String path = ctx.filePath.getText();
        path = path.substring(1, path.length() - 1);
        return new CommitTransformJobStatement(path);
    }

    @Override
    public Statement visitShowJobStatusStatement(ShowJobStatusStatementContext ctx) {
        long jobId = Long.parseLong(ctx.jobId.getText());
        return new ShowJobStatusStatement(jobId);
    }

    @Override
    public Statement visitCancelJobStatement(CancelJobStatementContext ctx) {
        long jobId = Long.parseLong(ctx.jobId.getText());
        return new CancelJobStatement(jobId);
    }

    @Override
    public Statement visitShowEligibleJobStatement(ShowEligibleJobStatementContext ctx) {
        JobState jobState = JobState.JOB_UNKNOWN;
        if (ctx.jobStatus().FINISHED() != null) {
            jobState = JobState.JOB_FINISHED;
        } else if (ctx.jobStatus().CREATED() != null) {
            jobState = JobState.JOB_CREATED;
        } else if (ctx.jobStatus().RUNNING() != null) {
            jobState = JobState.JOB_RUNNING;
        } else if (ctx.jobStatus().FAILING() != null) {
            jobState = JobState.JOB_FAILING;
        } else if (ctx.jobStatus().FAILED() != null) {
            jobState = JobState.JOB_FAILED;
        } else if (ctx.jobStatus().CLOSING() != null) {
            jobState = JobState.JOB_CLOSING;
        } else if (ctx.jobStatus().CLOSED() != null) {
            jobState = JobState.JOB_CLOSED;
        }
        return new ShowEligibleJobStatement(jobState);
    }

    @Override
    public Statement visitCompactStatement(CompactStatementContext ctx) {
        return new CompactStatement();
    }

    private void parseSelectPaths(SelectClauseContext ctx, SelectStatement selectStatement) {
        List<ExpressionContext> expressions = ctx.expression();

        for (ExpressionContext expr : expressions) {
            List<Expression> ret = parseExpression(expr, selectStatement);
            ret.forEach(expression -> {
                if (expression.getType().equals(ExpressionType.Constant)) {
                    // 当select一个不包含在表达式的常量时，这个常量会被看成selectedPath
                    String selectedPath = ((ConstantExpression) expression).getValue().toString();
                    selectStatement.setExpression(parseBaseExpression(selectedPath, selectStatement));
                } else {
                    selectStatement.setExpression(expression);
                }
            });
        }

        if (!selectStatement.getFuncTypeSet().isEmpty()) {
            selectStatement.setHasFunc(true);
        }
    }

    private List<Expression> parseExpression(ExpressionContext ctx,
        SelectStatement selectStatement) {
        if (ctx.path() != null) {
            return Collections.singletonList(parseBaseExpression(ctx, selectStatement));
        }
        if (ctx.constant() != null) {
            return Collections.singletonList(new ConstantExpression(parseValue(ctx.constant())));
        }

        List<Expression> ret = new ArrayList<>();
        if (ctx.inBracketExpr != null) {
            List<Expression> expressions = parseExpression(ctx.inBracketExpr, selectStatement);
            for (Expression expression : expressions) {
                ret.add(new BracketExpression(expression));
            }
        } else if (ctx.expr != null) {
            List<Expression> expressions = parseExpression(ctx.expr, selectStatement);
            Operator operator = parseOperator(ctx);
            for (Expression expression : expressions) {
                ret.add(new UnaryExpression(operator, expression));
            }
        } else if (ctx.leftExpr != null && ctx.rightExpr != null) {
            List<Expression> leftExpressions = parseExpression(ctx.leftExpr, selectStatement);
            List<Expression> rightExpressions = parseExpression(ctx.rightExpr, selectStatement);
            Operator operator = parseOperator(ctx);
            for (Expression leftExpression : leftExpressions) {
                for (Expression rightExpression : rightExpressions) {
                    ret.add(new BinaryExpression(leftExpression, rightExpression, operator));
                }
            }
        } else if (ctx.subquery() != null) {
            SelectStatement subStatement = new SelectStatement();
            subStatement.setIsSubQuery(true);
            parseQueryClause(ctx.subquery().queryClause(), subStatement);

            Filter filter;
            // TODO: check correlated
            filter = new BoolFilter(true);

            selectStatement.addSelectSubQueryPart(new SubQueryFromPart(subStatement, new JoinCondition(JoinType.SingleJoin, filter)));
            subStatement.getBaseExpressionMap().forEach((k, v) -> v.forEach(expression -> {
                String selectedPath;
                if (expression.hasAlias()) {
                    selectedPath = expression.getAlias();
                } else {
                    selectedPath = expression.getColumnName();
                }
                BaseExpression baseExpression = new BaseExpression(selectedPath);
                selectStatement.setSelectedFuncsAndPaths("", baseExpression, false);
                ret.add(baseExpression);
            }));
        } else {
            throw new SQLParserException("Illegal selected expression");
        }
        return ret;
    }

    private Expression parseBaseExpression(ExpressionContext ctx, SelectStatement selectStatement) {
        String funcName = "";
        if (ctx.functionName() != null) {
            funcName = ctx.functionName().getText();
        }

        String alias = "";
        if (ctx.asClause() != null) {
            alias = ctx.asClause().ID().getText();
        }

        String selectedPath = ctx.path().getText();

        // 如果查询语句中FROM子句只有一个部分且FROM一个前缀，则SELECT子句中的path只用写出后缀
        if (!selectStatement.hasJoinParts() && selectStatement.getFromParts().get(0).getType() == FromPartType.PathFromPart) {
            String fromPath = selectStatement.getFromParts().get(0).getPath();
            String fullPath = fromPath + SQLConstant.DOT + selectedPath;
            BaseExpression expression = new BaseExpression(fullPath, funcName, alias);
            selectStatement.setSelectedFuncsAndPaths(funcName, expression);
            return expression;
        } else {
            BaseExpression expression = new BaseExpression(selectedPath, funcName, alias);
            selectStatement.setSelectedFuncsAndPaths(funcName, expression);
            return expression;
        }
    }

    private Expression parseBaseExpression(String selectedPath, SelectStatement selectStatement) {
        // 如果查询语句不是一个子查询，FROM子句只有一个部分且FROM一个前缀，则WHERE条件中的path只用写出后缀
        if (!selectStatement.hasJoinParts() && selectStatement.getFromParts().get(0).getType() == FromPartType.PathFromPart) {
            String fromPath = selectStatement.getFromParts().get(0).getPath();
            String fullPath = fromPath + SQLConstant.DOT + selectedPath;
            BaseExpression expression = new BaseExpression(fullPath);
            selectStatement.setSelectedFuncsAndPaths("", expression);
            return expression;
        } else {
            BaseExpression expression = new BaseExpression(selectedPath);
            selectStatement.setSelectedFuncsAndPaths("", expression);
            return expression;
        }
    }

    private Operator parseOperator(ExpressionContext ctx) {
        if (ctx.STAR() != null) {
            return Operator.STAR;
        } else if (ctx.DIV() != null) {
            return Operator.DIV;
        } else if (ctx.MOD() != null) {
            return Operator.MOD;
        } else if (ctx.PLUS() != null) {
            return Operator.PLUS;
        } else if (ctx.MINUS() != null) {
            return Operator.MINUS;
        } else {
            throw new SQLParserException("Unknown operator in expression");
        }
    }

    private void parseSpecialClause(SpecialClauseContext ctx, SelectStatement selectStatement) {
        if (ctx.downsampleWithLevelClause() != null) {
            // downsampleWithLevelClause = downsampleClause + aggregateWithLevelClause
            parseDownsampleClause(ctx.downsampleWithLevelClause().downsampleClause(), selectStatement);
            parseAggregateWithLevelClause(ctx.downsampleWithLevelClause().aggregateWithLevelClause().INT(), selectStatement);
        }
        if (ctx.downsampleClause() != null) {
            parseDownsampleClause(ctx.downsampleClause(), selectStatement);
        }
        if (ctx.aggregateWithLevelClause() != null) {
            parseAggregateWithLevelClause(ctx.aggregateWithLevelClause().INT(), selectStatement);
        }
        if (ctx.groupByClause() != null) {
            parseGroupByClause(ctx.groupByClause(), selectStatement);
        }
        if (ctx.havingClause() != null) {
            Filter filter = parseOrExpression(ctx.havingClause().orExpression(), selectStatement, true);
            selectStatement.setHavingFilter(filter);
        }
        if (ctx.limitClause() != null) {
            Pair<Integer, Integer> limitAndOffset = parseLimitClause(ctx.limitClause());
            selectStatement.setLimit(limitAndOffset.getK());
            selectStatement.setOffset(limitAndOffset.getV());
        }
        if (ctx.orderByClause() != null) {
            parseOrderByClause(ctx.orderByClause(), selectStatement);
        }
    }

    private void parseDownsampleClause(DownsampleClauseContext ctx,
        SelectStatement selectStatement) {
        long precision = parseAggLen(ctx.aggLen(0));
        Pair<Long, Long> timeInterval = parseTimeInterval(ctx.timeInterval());
        selectStatement.setStartTime(timeInterval.k);
        selectStatement.setEndTime(timeInterval.v);
        selectStatement.setPrecision(precision);
        selectStatement.setSlideDistance(precision);
        selectStatement.setHasDownsample(true);
        if (ctx.STEP() != null) {
            long distance = parseAggLen(ctx.aggLen(1));
            selectStatement.setSlideDistance(distance);
        }

        // merge value filter and group time range filter
        KeyFilter startTime = new KeyFilter(Op.GE, timeInterval.k);
        KeyFilter endTime = new KeyFilter(Op.L, timeInterval.v);
        Filter mergedFilter;
        if (selectStatement.hasValueFilter()) {
            mergedFilter = new AndFilter(
                new ArrayList<>(Arrays.asList(selectStatement.getFilter(), startTime, endTime)));
        } else {
            mergedFilter = new AndFilter(new ArrayList<>(Arrays.asList(startTime, endTime)));
            selectStatement.setHasValueFilter(true);
        }
        selectStatement.setFilter(mergedFilter);
    }

    private void parseAggregateWithLevelClause(List<TerminalNode> layers,
        SelectStatement selectStatement) {
        if (!isSupportAggregateWithLevel(selectStatement)) {
            throw new SQLParserException(
                "Aggregate with level only support aggregate query count, sum, avg for now.");
        }
        layers.forEach(
            terminalNode -> selectStatement.setLayer(Integer.parseInt(terminalNode.getText())));
    }

    private void parseGroupByClause(GroupByClauseContext ctx, SelectStatement selectStatement) {
        selectStatement.setHasGroupBy(true);

        ctx.path().forEach(pathContext -> {
            String path;
            // 如果查询语句的FROM子句只有一个部分且FROM一个前缀，则GROUP BY后的path只用写出后缀
            if (!selectStatement.hasJoinParts() && selectStatement.getFromParts().get(0).getType() == FromPartType.PathFromPart) {
                path = selectStatement.getFromParts().get(0).getPath() + SQLConstant.DOT + pathContext.getText();
            } else {
                path = pathContext.getText();
            }
            if (path.contains("*")) {
                throw new SQLParserException(String
                    .format("GROUP BY path '%s' has '*', which is not supported.", path));
            }
            selectStatement.setGroupByPath(path);
            selectStatement.setPathSet(path);
        });

        selectStatement.getBaseExpressionMap().forEach((k, v) -> {
            if (k.equals("")) {
                v.forEach(expr -> {
                    if (!selectStatement.getGroupByPaths().contains(expr.getPathName())) {
                        throw new SQLParserException(
                            "Selected path must exist in group by clause.");
                    }
                });
            }
        });
    }

    private boolean isSupportAggregateWithLevel(SelectStatement selectStatement) {
        return supportedAggregateWithLevelFuncSet.containsAll(selectStatement.getFuncTypeSet());
    }

    // like standard SQL, limit N, M means limit M offset N
    private Pair<Integer, Integer> parseLimitClause(LimitClauseContext ctx) {
        int limit = Integer.MAX_VALUE;
        int offset = 0;
        if (ctx.INT().size() == 1) {
            limit = Integer.parseInt(ctx.INT(0).getText());
            if (ctx.offsetClause() != null) {
                offset = Integer.parseInt(ctx.offsetClause().INT().getText());
            }
        } else if (ctx.INT().size() == 2) {
            offset = Integer.parseInt(ctx.INT(0).getText());
            limit = Integer.parseInt(ctx.INT(1).getText());
        } else {
            throw new SQLParserException(
                "Parse limit clause error. Limit clause should like LIMIT M OFFSET N or LIMIT N, M.");
        }
        return new Pair<>(limit, offset);
    }

    private void parseOrderByClause(OrderByClauseContext ctx, SelectStatement selectStatement) {
        if (ctx.KEY() != null) {
            selectStatement.setOrderByPath(SQLConstant.KEY);
        }
        if (ctx.path() != null) {
            for (PathContext pathContext : ctx.path()) {
                String suffix = pathContext.getText(), prefix = selectStatement.getFromParts().get(0).getPath();
                String orderByPath;
                // 如果查询语句的FROM子句只有一个部分且FROM一个前缀，则ORDER BY后的path只用写出后缀
                if (!selectStatement.hasJoinParts() && selectStatement.getFromParts().get(0).getType() == FromPartType.PathFromPart) {
                    orderByPath = prefix + SQLConstant.DOT + suffix;
                } else {
                    orderByPath = suffix;
                }
                if (orderByPath.contains("*")) {
                    throw new SQLParserException(String
                        .format("ORDER BY path '%s' has '*', which is not supported.", orderByPath));
                }
                selectStatement.setOrderByPath(orderByPath);
            }
        }
        if (ctx.DESC() != null) {
            selectStatement.setAscending(false);
        }
    }

    private void parseAsClause(AsClauseContext ctx, SelectStatement selectStatement) {
        String aliasPrefix = ctx.ID().getText();
        selectStatement.setGlobalAlias(aliasPrefix);
        selectStatement.getBaseExpressionMap().forEach((k, v) -> v.forEach(expression -> {
            String alias = expression.getAlias();
            if (alias.equals("")) {
                alias = aliasPrefix + SQLConstant.DOT + expression.getColumnName();
            } else {
                alias = aliasPrefix + SQLConstant.DOT + alias;
            }
            expression.setAlias(alias);
        }));
    }

    private long parseAggLen(AggLenContext ctx) {
        if (ctx.TIME_WITH_UNIT() != null) {
            String durationStr = ctx.TIME_WITH_UNIT().getText();
            return TimeUtils.convertTimeWithUnitStrToLong(0, durationStr);
        } else {
            return Integer.parseInt(ctx.INT().getText());
        }
    }

    private Pair<Long, Long> parseTimeInterval(TimeIntervalContext interval) {
        long startTime, endTime;

        if (interval == null) {
            startTime = 0;
            endTime = Long.MAX_VALUE;
        } else {
            // use index +- 1 to implement [start, end], [start, end),
            // (start, end), (start, end] range in [start, end) interface.
            if (interval.LR_BRACKET() != null) { // (
                startTime = parseTime(interval.startTime) + 1;
            } else {
                startTime = parseTime(interval.startTime);
            }

            if (interval.RR_BRACKET() != null) { // )
                endTime = parseTime(interval.endTime);
            } else {
                endTime = parseTime(interval.endTime) + 1;
            }
        }

        if (startTime > endTime) {
            throw new SQLParserException(
                "Start time should be smaller than endTime in time interval.");
        }

        return new Pair<>(startTime, endTime);
    }

    private TagFilter parseWithClause(WithClauseContext ctx) {
        if (ctx.WITHOUT() != null) {
            return new WithoutTagFilter();
        } else if (ctx.orTagExpression() != null) {
            return parseOrTagExpression(ctx.orTagExpression());
        } else {
            return parseOrPreciseExpression(ctx.orPreciseExpression());
        }
    }

    private TagFilter parseOrTagExpression(OrTagExpressionContext ctx) {
        List<TagFilter> children = new ArrayList<>();
        for (AndTagExpressionContext andCtx : ctx.andTagExpression()) {
            children.add(parseAndTagExpression(andCtx));
        }
        return new OrTagFilter(children);
    }

    private TagFilter parseAndTagExpression(AndTagExpressionContext ctx) {
        List<TagFilter> children = new ArrayList<>();
        for (TagExpressionContext tagCtx : ctx.tagExpression()) {
            children.add(parseTagExpression(tagCtx));
        }
        return new AndTagFilter(children);
    }

    private TagFilter parseTagExpression(TagExpressionContext ctx) {
        if (ctx.orTagExpression() != null) {
            return parseOrTagExpression(ctx.orTagExpression());
        }
        String tagKey = ctx.tagKey().getText();
        String tagValue = ctx.tagValue().getText();
        return new BaseTagFilter(tagKey, tagValue);
    }

    private TagFilter parseOrPreciseExpression(OrPreciseExpressionContext ctx) {
        List<BasePreciseTagFilter> children = new ArrayList<>();
        for (AndPreciseExpressionContext tagCtx : ctx.andPreciseExpression()) {
            children.add(parseAndPreciseExpression(tagCtx));
        }
        return new PreciseTagFilter(children);
    }

    private BasePreciseTagFilter parseAndPreciseExpression(AndPreciseExpressionContext ctx) {
        Map<String, String> tagKVMap = new HashMap<>();
        for (PreciseTagExpressionContext tagCtx : ctx.preciseTagExpression()) {
            String tagKey = tagCtx.tagKey().getText();
            String tagValue = tagCtx.tagValue().getText();
            tagKVMap.put(tagKey, tagValue);
        }
        return new BasePreciseTagFilter(tagKVMap);
    }

    private Filter parseOrExpression(OrExpressionContext ctx, Statement statement) {
        return parseOrExpression(ctx, statement, false);
    }

    private Filter parseOrExpression(OrExpressionContext ctx, Statement statement, boolean isHavingFilter) {
        List<Filter> children = new ArrayList<>();
        for (AndExpressionContext andCtx : ctx.andExpression()) {
            children.add(parseAndExpression(andCtx, statement, isHavingFilter));
        }
        return children.size() == 1 ? children.get(0) : new OrFilter(children);
    }

    private Filter parseAndExpression(AndExpressionContext ctx, Statement statement, boolean isHavingFilter) {
        List<Filter> children = new ArrayList<>();
        for (PredicateContext predicateCtx : ctx.predicate()) {
            children.add(parsePredicate(predicateCtx, statement, isHavingFilter));
        }
        return children.size() == 1 ? children.get(0) : new AndFilter(children);
    }

    private Filter parsePredicate(PredicateContext ctx, Statement statement, boolean isHavingFilter) {
        if (ctx.orExpression() != null) {
            Filter filter = parseOrExpression(ctx.orExpression(), statement, isHavingFilter);
            return ctx.OPERATOR_NOT() == null ? filter : new NotFilter(filter);
        } else {
            if (ctx.path().size() == 0 && ctx.predicateWithSubquery() == null) {
                return parseKeyFilter(ctx);
            } else {
                StatementType type = statement.getType();
                if (type != StatementType.SELECT) {
                    throw new SQLParserException(
                        String.format("%s clause can not use value or path filter.",
                            type.toString().toLowerCase())
                    );
                }

                if (ctx.predicateWithSubquery() != null) {
                    return parseFilterWithSubQuery(ctx.predicateWithSubquery(), (SelectStatement) statement, isHavingFilter);
                } else if (ctx.path().size() == 1) {
                    return parseValueFilter(ctx, (SelectStatement) statement);
                } else {
                    return parsePathFilter(ctx, (SelectStatement) statement);
                }
            }
        }
    }

    private KeyFilter parseKeyFilter(PredicateContext ctx) {
        Op op = Op.str2Op(ctx.comparisonOperator().getText());
        // deal with sub clause like 100 < key
        if (ctx.children.get(0) instanceof ConstantContext) {
            op = Op.getDirectionOpposite(op);
        }
        long time = (long) parseValue(ctx.constant());
        return new KeyFilter(op, time);
    }

    private Filter parseValueFilter(PredicateContext ctx, SelectStatement statement) {
        String path = ctx.path().get(0).getText();
        // 如果查询语句不是一个子查询，FROM子句只有一个部分且FROM一个前缀，则WHERE条件中的path只用写出后缀
        if (!statement.hasJoinParts() && !statement.isSubQuery()
                && statement.getFromParts().get(0).getType() == FromPartType.PathFromPart) {
            path = statement.getFromParts().get(0).getPath() + SQLConstant.DOT + path;
        }
        statement.setPathSet(path);

        // deal with having filter with functions like having avg(a) > 3.
        // we need a instead of avg(a) to combine fragments' raw data.
        if (ctx.functionName() != null) {
            path = ctx.functionName().getText() + "(" + path + ")";
        }

        Op op;
        if (ctx.OPERATOR_LIKE() != null) {
            op = Op.LIKE;
        } else {
            op = Op.str2Op(ctx.comparisonOperator().getText().trim().toLowerCase());
            // deal with sub clause like 100 < path
            if (ctx.children.get(0) instanceof ConstantContext) {
                op = Op.getDirectionOpposite(op);
            }
        }

        Value value;
        if (ctx.regex != null) {
            String regex = ctx.regex.getText();
            value = new Value(regex.substring(1, regex.length() - 1));
        } else {
            value = new Value(parseValue(ctx.constant()));
        }

        return new ValueFilter(path, op, value);
    }

    private Filter parsePathFilter(PredicateContext ctx, SelectStatement statement) {
        String pathA = ctx.path().get(0).getText();
        String pathB = ctx.path().get(1).getText();

        Op op = Op.str2Op(ctx.comparisonOperator().getText().trim().toLowerCase());

        // 如果查询语句不是一个子查询，FROM子句只有一个部分且FROM一个前缀，则WHERE条件中的path只用写出后缀
        if (!statement.hasJoinParts() && !statement.isSubQuery()
                && statement.getFromParts().get(0).getType() == FromPartType.PathFromPart) {
            pathA = statement.getFromParts().get(0).getPath() + SQLConstant.DOT + pathA;
            pathB = statement.getFromParts().get(0).getPath() + SQLConstant.DOT + pathB;
        }
        statement.setPathSet(pathA);
        statement.setPathSet(pathB);
        return new PathFilter(pathA, op, pathB);
    }

    private Filter parseFilterWithSubQuery(PredicateWithSubqueryContext ctx, SelectStatement statement, boolean isHavingFilter) {
        if (ctx.EXISTS() != null) {
            return parseExistsFilter(ctx, statement, isHavingFilter);
        } else if (ctx.IN() != null) {
            return parseInFilter(ctx, statement, isHavingFilter);
        } else if (ctx.quantifier() != null) {
            return parseQuantifierComparisonFilter(ctx, statement, isHavingFilter);
        } else {
            if (ctx.subquery().size() == 1) {
                return parseScalarSubQueryComparisonFilter(ctx, statement, isHavingFilter);
            } else {
                return parseTwoScalarSubQueryComparisonFilter(ctx, statement, isHavingFilter);
            }
        }
    }

    private Filter parseExistsFilter(PredicateWithSubqueryContext ctx, SelectStatement statement, boolean isHavingFilter) {
        SelectStatement subStatement = new SelectStatement();
        subStatement.setIsSubQuery(true);
        parseQueryClause(ctx.subquery().get(0).queryClause(), subStatement);
        String markColumn = "&mark" + markJoinCount;
        markJoinCount += 1;

        Filter filter;
        // TODO: check correlated
        filter = new BoolFilter(true);

        boolean isAntiJoin = ctx.OPERATOR_NOT() != null;
        SubQueryFromPart subQueryPart = new SubQueryFromPart(subStatement, new JoinCondition(JoinType.MarkJoin, filter, markColumn, isAntiJoin));
        if (isHavingFilter) {
            statement.addHavingSubQueryPart(subQueryPart);
        } else {
            statement.addWhereSubQueryPart(subQueryPart);
        }
        return new ValueFilter(markColumn, Op.E, new Value(true));
    }

    private Filter parseInFilter(PredicateWithSubqueryContext ctx, SelectStatement statement, boolean isHavingFilter) {
        SelectStatement subStatement = new SelectStatement();
        subStatement.setIsSubQuery(true);
        parseQueryClause(ctx.subquery().get(0).queryClause(), subStatement);
        if (subStatement.getExpressions().size() != 1) {
            throw new SQLParserException("The number of columns in sub-query doesn't equal to outer row.");
        }
        String markColumn = "&mark" + markJoinCount;
        markJoinCount += 1;

        Filter filter;
        if (ctx.constant() != null) {
            Value value = new Value(parseValue(ctx.constant()));
            String path = subStatement.getExpressions().get(0).getColumnName();
            filter = new ValueFilter(path, Op.E, value);
        } else {
            String pathA = ctx.path().getText();
            if (!statement.hasJoinParts() && !statement.isSubQuery()
                    && statement.getFromParts().get(0).getType() == FromPartType.PathFromPart) {
                pathA = statement.getFromParts().get(0).getPath() + SQLConstant.DOT + pathA;
            }
            // deal with having filter with functions
            if (ctx.functionName() != null) {
                pathA = ctx.functionName().getText() + "(" + pathA + ")";
            }

            String pathB = subStatement.getExpressions().get(0).getColumnName();
            filter = new PathFilter(pathA, Op.E, pathB);
        }
        // TODO: check correlated

        boolean isAntiJoin = ctx.OPERATOR_NOT() != null;
        SubQueryFromPart subQueryPart = new SubQueryFromPart(subStatement, new JoinCondition(JoinType.MarkJoin, filter, markColumn, isAntiJoin));
        if (isHavingFilter) {
            statement.addHavingSubQueryPart(subQueryPart);
        } else {
            statement.addWhereSubQueryPart(subQueryPart);
        }
        return new ValueFilter(markColumn, Op.E, new Value(true));
    }

    private Filter parseQuantifierComparisonFilter(PredicateWithSubqueryContext ctx, SelectStatement statement, boolean isHavingFilter) {
        SelectStatement subStatement = new SelectStatement();
        subStatement.setIsSubQuery(true);
        parseQueryClause(ctx.subquery().get(0).queryClause(), subStatement);
        if (subStatement.getExpressions().size() != 1) {
            throw new SQLParserException("The number of columns in sub-query doesn't equal to outer row.");
        }
        String markColumn = "&mark" + markJoinCount;
        markJoinCount += 1;

        Filter filter;
        Op op = Op.str2Op(ctx.comparisonOperator().getText().trim().toLowerCase());
        if (ctx.quantifier().all() != null) {
            op = Op.getOpposite(op);
        }

        if (ctx.constant() != null) {
            Value value = new Value(parseValue(ctx.constant()));
            String path = subStatement.getExpressions().get(0).getColumnName();
            filter = new ValueFilter(path, op, value);
        } else {
            String pathA = ctx.path().getText();
            if (!statement.hasJoinParts() && !statement.isSubQuery()
                    && statement.getFromParts().get(0).getType() == FromPartType.PathFromPart) {
                pathA = statement.getFromParts().get(0).getPath() + SQLConstant.DOT + pathA;
            }
            // deal with having filter with functions
            if (ctx.functionName() != null) {
                pathA = ctx.functionName().getText() + "(" + pathA + ")";
            }

            String pathB = subStatement.getExpressions().get(0).getColumnName();
            filter = new PathFilter(pathA, op, pathB);
        }
        // TODO: check correlated

        boolean isAntiJoin = ctx.quantifier().all() != null;
        SubQueryFromPart subQueryPart = new SubQueryFromPart(subStatement, new JoinCondition(JoinType.MarkJoin, filter, markColumn, isAntiJoin));
        if (isHavingFilter) {
            statement.addHavingSubQueryPart(subQueryPart);
        } else {
            statement.addWhereSubQueryPart(subQueryPart);
        }
        return new ValueFilter(markColumn, Op.E, new Value(true));
    }

    private Filter parseScalarSubQueryComparisonFilter(PredicateWithSubqueryContext ctx, SelectStatement statement, boolean isHavingFilter) {
        SelectStatement subStatement = new SelectStatement();
        subStatement.setIsSubQuery(true);
        parseQueryClause(ctx.subquery().get(0).queryClause(), subStatement);
        if (subStatement.getExpressions().size() != 1) {
            throw new SQLParserException("The number of columns in sub-query doesn't equal to outer row.");
        }

        Filter filter;
        // TODO: check correlated
        filter = new BoolFilter(true);

        SubQueryFromPart subQueryPart = new SubQueryFromPart(subStatement, new JoinCondition(JoinType.SingleJoin, filter));
        if (isHavingFilter) {
            statement.addHavingSubQueryPart(subQueryPart);
        } else {
            statement.addWhereSubQueryPart(subQueryPart);
        }

        Op op = Op.str2Op(ctx.comparisonOperator().getText().trim().toLowerCase());
        if (ctx.constant() != null) {
            Value value = new Value(parseValue(ctx.constant()));
            String path = subStatement.getExpressions().get(0).getColumnName();
            return new ValueFilter(path, op, value);
        } else {
            String pathA = ctx.path().getText();
            if (!statement.hasJoinParts() && !statement.isSubQuery()
                    && statement.getFromParts().get(0).getType() == FromPartType.PathFromPart) {
                pathA = statement.getFromParts().get(0).getPath() + SQLConstant.DOT + pathA;
            }
            // deal with having filter with functions
            if (ctx.functionName() != null) {
                pathA = ctx.functionName().getText() + "(" + pathA + ")";
            }

            String pathB = subStatement.getExpressions().get(0).getColumnName();
            return new PathFilter(pathA, op, pathB);
        }
    }

    private Filter parseTwoScalarSubQueryComparisonFilter(PredicateWithSubqueryContext ctx, SelectStatement statement, boolean isHavingFilter) {
        List<String> paths = new ArrayList<>();

        for (int i = 0; i < 2; i++) {
            SelectStatement subStatement = new SelectStatement();
            subStatement.setIsSubQuery(true);
            parseQueryClause(ctx.subquery().get(i).queryClause(), subStatement);
            if (subStatement.getExpressions().size() != 1) {
                throw new SQLParserException("The number of columns in sub-query doesn't equal to outer row.");
            }
            paths.add(subStatement.getExpressions().get(0).getColumnName());

            Filter filter;
            // TODO: check correlated
            filter = new BoolFilter(true);

            SubQueryFromPart subQueryPart = new SubQueryFromPart(subStatement, new JoinCondition(JoinType.SingleJoin, filter));
            if (isHavingFilter) {
                statement.addHavingSubQueryPart(subQueryPart);
            } else {
                statement.addWhereSubQueryPart(subQueryPart);
            }
        }

        Op op = Op.str2Op(ctx.comparisonOperator().getText().trim().toLowerCase());
        return new PathFilter(paths.get(0), op, paths.get(1));
    }

    private Map<String, String> parseExtra(StringLiteralContext ctx) {
        Map<String, String> map = new HashMap<>();
        String extra = ctx.getText().trim();
        if (extra.length() == 0 || extra.equals(SQLConstant.DOUBLE_QUOTES)) {
            return map;
        }
        extra = extra
            .substring(extra.indexOf(SQLConstant.QUOTE) + 1, extra.lastIndexOf(SQLConstant.QUOTE));
        String[] kvStr = extra.split(SQLConstant.COMMA);
        for (String kv : kvStr) {
            String[] kvArray = kv.split(SQLConstant.COLON);
            if (kvArray.length != 2) {
                if (kv.contains("url")) {
                    map.put("url", kv.substring(kv.indexOf(":") + 1));
                }
                continue;
            }
            map.put(kvArray[0].trim(), kvArray[1].trim());
        }
        return map;
    }

    private void parseInsertValuesSpec(InsertValuesSpecContext ctx,
        InsertStatement insertStatement) {
        List<InsertMultiValueContext> insertMultiValues = ctx.insertMultiValue();

        int size = insertMultiValues.size();
        int vSize = insertMultiValues.get(0).constant().size();
        Long[] times = new Long[size];
        Object[][] values = new Object[vSize][size];
        DataType[] types = new DataType[vSize];

        for (int i = 0; i < insertMultiValues.size(); i++) {
            times[i] = parseTime(insertMultiValues.get(i).timeValue());

            List<ConstantContext> constants = insertMultiValues.get(i).constant();
            for (int j = 0; j < constants.size(); j++) {
                values[j][i] = parseValue(constants.get(j));
            }
        }

        // tricky implements, values may be NaN or Null
        int count = 0;
        for (int i = 0; i < insertMultiValues.size(); i++) {
            if (count == types.length) {
                break;
            }
            List<ConstantContext> constants = insertMultiValues.get(i).constant();
            for (int j = 0; j < constants.size(); j++) {
                ConstantContext cons = constants.get(j);
                if (cons.NULL() == null && cons.NaN() == null && types[j] == null) {
                    types[j] = parseType(cons);
                    if (types[j] != null) {
                        count++;
                    }
                }
            }
        }

        insertStatement.setTimes(new ArrayList<>(Arrays.asList(times)));
        insertStatement.setValues(values);
        insertStatement.setTypes(new ArrayList<>(Arrays.asList(types)));
    }

    private Object parseValue(ConstantContext ctx) {
        if (ctx.booleanClause() != null) {
            return Boolean.parseBoolean(ctx.booleanClause().getText());
        } else if (ctx.dateExpression() != null) {
            return parseDateExpression(ctx.dateExpression());
        } else if (ctx.stringLiteral() != null) {
            // trim, "str" may look like ""str"".
            // Attention!! DataType in thrift interface only! support! binary!
            String str = ctx.stringLiteral().getText();
            return str.substring(1, str.length() - 1).getBytes();
        } else if (ctx.realLiteral() != null) {
            // maybe contains minus, see Sql.g4 for more details.
            return Double.parseDouble(ctx.getText());
        } else if (ctx.INT() != null) {
            // INT() may NOT IN [-2147483648, 2147483647], see Sql.g4 for more details.
            return Long.parseLong(ctx.getText());
        } else {
            return null;
        }
    }

    private DataType parseType(ConstantContext ctx) {
        if (ctx.booleanClause() != null) {
            return DataType.BOOLEAN;
        } else if (ctx.dateExpression() != null) {
            // data expression will be auto transform to Long.
            return DataType.LONG;
        } else if (ctx.stringLiteral() != null) {
            return DataType.BINARY;
        } else if (ctx.realLiteral() != null) {
            return DataType.DOUBLE;
        } else if (ctx.INT() != null) {
            // INT() may NOT IN [-2147483648, 2147483647], see Sql.g4 for more details.
            return DataType.LONG;
        } else {
            return null;
        }
    }

    private long parseTime(TimeValueContext time) {
        long timeInNs;
        if (time.INT() != null) {
            timeInNs = Long.parseLong(time.INT().getText());
        } else if (time.dateExpression() != null) {
            timeInNs = parseDateExpression(time.dateExpression());
        } else if (time.dateFormat() != null) {
            timeInNs = parseTimeFormat(time.dateFormat());
        } else if (time.getText().equalsIgnoreCase(SQLConstant.INF)) {
            timeInNs = Long.MAX_VALUE;
        } else {
            timeInNs = Long.MIN_VALUE;
        }
        return timeInNs;
    }

    private long parseDateExpression(DateExpressionContext ctx) {
        long time;
        time = parseTimeFormat(ctx.dateFormat());
        for (int i = 1; i < ctx.getChildCount(); i = i + 2) {
            if (ctx.getChild(i).getText().equals(SQLConstant.PLUS)) {
                time += TimeUtils.convertTimeWithUnitStrToLong(time, ctx.getChild(i + 1).getText());
            } else {
                time -= TimeUtils.convertTimeWithUnitStrToLong(time, ctx.getChild(i + 1).getText());
            }
        }
        return time;
    }

    private long parseTimeFormat(DateFormatContext ctx) throws SQLParserException {
        if (ctx.NOW() != null) {
            return System.nanoTime();
        }
        if (ctx.TIME_WITH_UNIT() != null) {
            return TimeUtils.convertTimeWithUnitStrToLong(0, ctx.getText());
        }
        try {
            return TimeUtils.convertDatetimeStrToLong(ctx.getText());
        } catch (Exception e) {
            throw new SQLParserException(
                String.format("Input time format %s error. ", ctx.getText()));
        }
    }

    private Map<String, String> parseTagList(TagListContext ctx) {
        Map<String, String> tags = new HashMap<>();
        for (TagEquationContext tagCtx : ctx.tagEquation()) {
            String tagKey = tagCtx.tagKey().getText();
            String tagValue = tagCtx.tagValue().getText();
            tags.put(tagKey, tagValue);
        }
        return tags;
    }

}
