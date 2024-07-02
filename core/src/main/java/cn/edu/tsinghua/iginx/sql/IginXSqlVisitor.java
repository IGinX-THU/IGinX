package cn.edu.tsinghua.iginx.sql;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.KEY_MAX_VAL;
import static cn.edu.tsinghua.iginx.constant.GlobalConstant.KEY_MIN_VAL;
import static cn.edu.tsinghua.iginx.engine.shared.function.FunctionUtils.isCanUseSetQuantifierFunction;
import static cn.edu.tsinghua.iginx.engine.shared.operator.MarkJoin.MARK_PREFIX;
import static cn.edu.tsinghua.iginx.sql.statement.select.SelectStatement.markJoinCount;

import cn.edu.tsinghua.iginx.engine.logical.utils.LogicalFilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType;
import cn.edu.tsinghua.iginx.engine.shared.expr.BaseExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.BinaryExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.BracketExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.ConstantExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.Expression;
import cn.edu.tsinghua.iginx.engine.shared.expr.FromValueExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.FuncExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.Operator;
import cn.edu.tsinghua.iginx.engine.shared.expr.UnaryExpression;
import cn.edu.tsinghua.iginx.engine.shared.file.CSVFile;
import cn.edu.tsinghua.iginx.engine.shared.file.read.ImportCsv;
import cn.edu.tsinghua.iginx.engine.shared.file.read.ImportFile;
import cn.edu.tsinghua.iginx.engine.shared.file.write.ExportByteStream;
import cn.edu.tsinghua.iginx.engine.shared.file.write.ExportCsv;
import cn.edu.tsinghua.iginx.engine.shared.file.write.ExportFile;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.AndTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.BasePreciseTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.BaseTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.OrTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.PreciseTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.WithoutTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.sql.SqlParser.AddStorageEngineStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.AggLenContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.AndExpressionContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.AndPreciseExpressionContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.AndTagExpressionContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.CancelJobStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ClearDataStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.CommitTransformJobStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.CommonTableExprContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.CompactStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ConstantContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.CountPointsStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.CsvFileContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.CteClauseContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.CteColumnContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.DateExpressionContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.DateFormatContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.DeleteColumnsStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.DeleteStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.DownsampleClauseContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.DropTaskStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ExportFileClauseContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ExpressionContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.FromClauseContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.FunctionContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.GroupByClauseContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ImportFileClauseContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.InsertFromFileStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.InsertFullPathSpecContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.InsertMultiValueContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.InsertStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.InsertValuesSpecContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.JoinContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.JoinPartContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.LimitClauseContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.NodeNameContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.OrExpressionContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.OrPreciseExpressionContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.OrTagExpressionContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.OrderByClauseContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ParamContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.PathContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.PreciseTagExpressionContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.PredicateContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.PredicateWithSubqueryContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.QueryClauseContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.RegisterTaskStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.RemoveHistoryDataSourceStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.SelectClauseContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.SelectContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.SelectStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.SelectSublistContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.SetConfigStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.SetRulesStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ShowClusterInfoStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ShowColumnsOptionsContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ShowColumnsStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ShowConfigStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ShowEligibleJobStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ShowJobStatusStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ShowRegisterTaskStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ShowReplicationStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ShowRulesStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ShowSessionIDStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.SpecialClauseContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.SqlStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.StorageEngineContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.StringLiteralContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.TableReferenceContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.TagEquationContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.TagExpressionContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.TagListContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.TimeIntervalContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.TimeValueContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.WithClauseContext;
import cn.edu.tsinghua.iginx.sql.exception.SQLParserException;
import cn.edu.tsinghua.iginx.sql.statement.*;
import cn.edu.tsinghua.iginx.sql.statement.frompart.CteFromPart;
import cn.edu.tsinghua.iginx.sql.statement.frompart.FromPart;
import cn.edu.tsinghua.iginx.sql.statement.frompart.FromPartType;
import cn.edu.tsinghua.iginx.sql.statement.frompart.PathFromPart;
import cn.edu.tsinghua.iginx.sql.statement.frompart.ShowColumnsFromPart;
import cn.edu.tsinghua.iginx.sql.statement.frompart.SubQueryFromPart;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinType;
import cn.edu.tsinghua.iginx.sql.statement.select.BinarySelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.select.CommonTableExpression;
import cn.edu.tsinghua.iginx.sql.statement.select.SelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.select.UnarySelectStatement;
import cn.edu.tsinghua.iginx.sql.utils.ExpressionUtils;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.TimeUtils;
import java.util.*;
import java.util.stream.Collectors;

public class IginXSqlVisitor extends SqlBaseVisitor<Statement> {

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

    parseInsertFullPathSpec(ctx.insertFullPathSpec(), insertStatement);

    InsertValuesSpecContext valuesSpecContext = ctx.insertValuesSpec();
    if (hasSubQuery) {
      List<CommonTableExpression> cteList = new ArrayList<>();
      if (ctx.insertValuesSpec().cteClause() != null) {
        parseCteClause(ctx.insertValuesSpec().cteClause(), cteList);
      }
      SelectStatement selectStatement =
          parseQueryClause(ctx.insertValuesSpec().queryClause(), cteList, false);
      long timeOffset =
          valuesSpecContext.TIME_OFFSET() == null
              ? 0
              : Long.parseLong(valuesSpecContext.INT().getText());
      return new InsertFromSelectStatement(timeOffset, selectStatement, insertStatement);
    } else {
      // parse keys, values and types
      parseInsertValuesSpec(valuesSpecContext, insertStatement);
      if (insertStatement.getPaths().size() != insertStatement.getValues().length) {
        throw new SQLParserException("Insert path size and value size must be equal.");
      }
      insertStatement.sortData();
      return insertStatement;
    }
  }

  @Override
  public Statement visitInsertFromFileStatement(InsertFromFileStatementContext ctx) {
    ImportFile importFile = parseImportFileClause(ctx.importFileClause());

    InsertStatement insertStatement = new InsertStatement(RawDataType.NonAlignedRow);
    if (ctx.insertFullPathSpec() == null)
      parsePartialPathSpec(ctx.path(), ctx.tagList(), insertStatement);
    else parseInsertFullPathSpec(ctx.insertFullPathSpec(), insertStatement);

    long keyBase = ctx.keyBase == null ? 0 : Long.parseLong(ctx.keyBase.getText());
    String keyCol =
        ctx.keyName == null
            ? null
            : ctx.keyName.getText().substring(1, ctx.keyName.getText().length() - 1);
    return new InsertFromCsvStatement(importFile, insertStatement, keyBase, keyCol);
  }

  private void parsePartialPathSpec(
      PathContext pathCtx, TagListContext tagCtx, InsertStatement insertStatement) {
    insertStatement.setPrefixPath(parsePath(pathCtx));

    if (tagCtx != null) {
      Map<String, String> globalTags = parseTagList(tagCtx);
      insertStatement.setGlobalTags(globalTags);
    }
  }

  private void parseInsertFullPathSpec(
      InsertFullPathSpecContext ctx, InsertStatement insertStatement) {
    insertStatement.setPrefixPath(parsePath(ctx.path()));

    if (ctx.tagList() != null) {
      Map<String, String> globalTags = parseTagList(ctx.tagList());
      insertStatement.setGlobalTags(globalTags);
    }
    // parse paths
    Set<Pair<String, Map<String, String>>> columnsSet = new HashSet<>();
    ctx.insertColumnsSpec()
        .insertPath()
        .forEach(
            e -> {
              String path = parsePath(e.path());
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
  }

  private String parsePath(PathContext ctx) {
    StringBuilder path = new StringBuilder();
    for (NodeNameContext nodeNameContext : ctx.nodeName()) {
      String nodeName = nodeNameContext.getText();
      if (nodeNameContext.BACK_QUOTE_STRING_LITERAL_NOT_EMPTY() != null) {
        nodeName = nodeName.substring(1, nodeName.length() - 1);
      }
      path.append(nodeName);
      path.append(SQLConstant.DOT);
    }
    path.deleteCharAt(path.length() - 1);
    return path.toString();
  }

  private ImportFile parseImportFileClause(ImportFileClauseContext ctx) {
    if (ctx.csvFile() != null) {
      String filePath = ctx.csvFile().filePath.getText();
      filePath = filePath.substring(1, filePath.length() - 1);
      ImportCsv importCsv = new ImportCsv(filePath);
      parseCsvFile(ctx.csvFile(), importCsv.getCsvFile());
      if (ctx.HEADER() != null) {
        importCsv.setSkippingImportHeader(true);
      }
      return importCsv;
    } else {
      throw new SQLParserException("Unknown import file type");
    }
  }

  @Override
  public Statement visitDeleteStatement(DeleteStatementContext ctx) {
    DeleteStatement deleteStatement = new DeleteStatement();
    // parse delete paths
    ctx.path().forEach(e -> deleteStatement.addPath(parsePath(e)));
    // parse time range
    if (ctx.whereClause() != null) {
      Filter filter =
          parseOrExpression(ctx.whereClause().orExpression(), deleteStatement).getFilter();
      deleteStatement.setKeyRangesByFilter(filter);
    } else {
      List<KeyRange> keyRanges =
          new ArrayList<>(Collections.singletonList(new KeyRange(0, Long.MAX_VALUE)));
      deleteStatement.setKeyRanges(keyRanges);
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
    List<CommonTableExpression> cteList = new ArrayList<>();
    if (ctx.cteClause() != null) {
      parseCteClause(ctx.cteClause(), cteList);
    }

    SelectStatement selectStatement = parseQueryClause(ctx.queryClause(), cteList, false);
    if (ctx.EXPLAIN() != null) {
      if (ctx.PHYSICAL() != null) {
        selectStatement.setNeedPhysicalExplain(true);
      } else {
        selectStatement.setNeedLogicalExplain(true);
      }
    }
    if (ctx.orderByClause() != null) {
      parseOrderByClause(ctx.orderByClause(), selectStatement);
    }
    if (ctx.limitClause() != null) {
      parseLimitClause(ctx.limitClause(), selectStatement);
    }
    if (ctx.exportFileClause() != null) {
      if (ctx.EXPLAIN() != null) {
        throw new SQLParserException(
            "OUTFILE is not supported to be used in statement with EXPLAIN.");
      }
      ExportFile exportFile = parseExportFileClause(ctx.exportFileClause());
      return new ExportFileFromSelectStatement(selectStatement, exportFile);
    }
    return selectStatement;
  }

  private void parseCteClause(CteClauseContext ctx, List<CommonTableExpression> cteList) {
    for (CommonTableExprContext cteCtx : ctx.commonTableExpr()) {
      String name = cteCtx.cteName().getText();
      SelectStatement statement = parseQueryClause(cteCtx.queryClause(), cteList, false);
      if (cteCtx.orderByClause() != null) {
        parseOrderByClause(cteCtx.orderByClause(), statement);
      }
      if (cteCtx.limitClause() != null) {
        parseLimitClause(cteCtx.limitClause(), statement);
      }
      if (cteCtx.columnsList() != null) {
        List<String> columns = new ArrayList<>();
        for (CteColumnContext columnCtx : cteCtx.columnsList().cteColumn()) {
          columns.add(columnCtx.getText());
        }
        if (columns.size() != statement.getExpressions().size()) {
          throw new SQLParserException(
              String.format(
                  "CTE %s's renamed columns' size is %s, while selected expressions' size is %s",
                  name, columns.size(), statement.getExpressions().size()));
        }
        cteList.add(new CommonTableExpression(statement, name, columns));
      } else {
        cteList.add(new CommonTableExpression(statement, name));
      }
    }
  }

  private SelectStatement parseQueryClause(
      QueryClauseContext ctx, List<CommonTableExpression> cteList, boolean isSubQuery) {
    if (ctx.inBracketQuery != null) {
      SelectStatement selectStatement = parseQueryClause(ctx.inBracketQuery, cteList, isSubQuery);
      if (ctx.orderByClause() != null) {
        parseOrderByClause(ctx.orderByClause(), selectStatement);
      }
      if (ctx.limitClause() != null) {
        parseLimitClause(ctx.limitClause(), selectStatement);
      }
      return selectStatement;
    } else if (ctx.leftQuery != null && ctx.rightQuery != null) {
      SelectStatement leftSelectStatement = parseQueryClause(ctx.leftQuery, cteList, isSubQuery);
      SelectStatement rightSelectStatement = parseQueryClause(ctx.rightQuery, cteList, isSubQuery);
      OperatorType setOperator = parseSetOperator(ctx);
      boolean isDistinct = ctx.ALL() == null;
      BinarySelectStatement statement =
          new BinarySelectStatement(
              leftSelectStatement, setOperator, rightSelectStatement, isDistinct, isSubQuery);
      statement.setCteList(cteList);
      return statement;
    } else {
      UnarySelectStatement selectStatement = new UnarySelectStatement(isSubQuery);
      selectStatement.setCteList(cteList);
      parseSelect(ctx.select(), selectStatement);
      return selectStatement;
    }
  }

  private OperatorType parseSetOperator(QueryClauseContext ctx) {
    if (ctx.INTERSECT() != null) {
      return OperatorType.Intersect;
    } else if (ctx.UNION() != null) {
      return OperatorType.Union;
    } else if (ctx.EXCEPT() != null) {
      return OperatorType.Except;
    } else {
      throw new SQLParserException("Unknown set operator in statement");
    }
  }

  private void parseSelect(SelectContext ctx, UnarySelectStatement selectStatement) {
    // Step 1. parse as much information as possible.
    // parse from paths
    if (ctx.fromClause() != null) {
      parseFromParts(ctx.fromClause(), selectStatement);
    }
    // parse select paths
    if (ctx.selectClause() != null) {
      parseSelectPaths(ctx.selectClause(), selectStatement);
    }
    // parse where clause
    if (ctx.whereClause() != null) {
      FilterData filterData = parseOrExpression(ctx.whereClause().orExpression(), selectStatement);
      Filter filter = LogicalFilterUtils.removeSingleFilter(filterData.getFilter());
      selectStatement.setFilter(filter);
      selectStatement.setHasValueFilter(true);
      filterData.getPathList().forEach(selectStatement::addWherePath);
      filterData.getSubQueryFromPartList().forEach(selectStatement::addWhereSubQueryPart);
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

    // Step 2. decide the query type according to the information.
    selectStatement.checkQueryType();
  }

  private ExportFile parseExportFileClause(ExportFileClauseContext ctx) {
    if (ctx.csvFile() != null) {
      String filePath = ctx.csvFile().filePath.getText();
      filePath = filePath.substring(1, filePath.length() - 1);
      ExportCsv exportCsv = new ExportCsv(filePath);
      parseCsvFile(ctx.csvFile(), exportCsv.getCsvFile());
      if (ctx.HEADER() != null) {
        exportCsv.setExportHeader(true);
      }
      return exportCsv;
    } else if (ctx.streamFile() != null) {
      String dirPath = ctx.streamFile().dirPath.getText();
      dirPath = dirPath.substring(1, dirPath.length() - 1);
      return new ExportByteStream(dirPath);
    } else {
      throw new SQLParserException("Unknown export file type");
    }
  }

  private void parseCsvFile(CsvFileContext ctx, CSVFile csvFile) {
    if (ctx.fieldsOption() != null) {
      if (ctx.fieldsOption().TERMINATED() == null
          && ctx.fieldsOption().ENCLOSED() == null
          && ctx.fieldsOption().ESCAPED() == null) {
        throw new SQLParserException("FILEDS should be used with TERMINATED, ENCLOSED or ESCAPED.");
      }
      if (ctx.fieldsOption().TERMINATED() != null) {
        String delimiter = ctx.fieldsOption().fieldsTerminated.getText();
        delimiter = delimiter.substring(1, delimiter.length() - 1);
        csvFile.setDelimiter(delimiter);
      }
      if (ctx.fieldsOption().ENCLOSED() != null) {
        String quote = ctx.fieldsOption().enclosed.getText();
        quote = quote.substring(1, quote.length() - 1);
        if (quote.length() != 1) {
          throw new SQLParserException("a char is expected behind ENCLOSED BY.");
        }
        csvFile.setQuote(quote.charAt(0));
        if (ctx.fieldsOption().OPTIONALLY() != null) {
          csvFile.setOptionallyQuote(true);
        }
      }
      if (ctx.fieldsOption().ESCAPED() != null) {
        String escaped = ctx.fieldsOption().escaped.getText();
        escaped = escaped.substring(1, escaped.length() - 1);
        if (escaped.length() != 1) {
          throw new SQLParserException("a char is expected behind ENCLOSED BY.");
        }
        csvFile.setEscaped(escaped.charAt(0));
      }
    }

    if (ctx.linesOption() != null) {
      String recordSeparator = ctx.linesOption().linesTerminated.getText();
      recordSeparator = recordSeparator.substring(1, recordSeparator.length() - 1);
      String CRLF = "\r\n";
      csvFile.setRecordSeparator(recordSeparator);
    }
  }

  @Override
  public Statement visitDeleteColumnsStatement(DeleteColumnsStatementContext ctx) {
    DeleteColumnsStatement deleteColumnsStatement = new DeleteColumnsStatement();
    ctx.path().forEach(e -> deleteColumnsStatement.addPath(parsePath(e)));

    if (ctx.withClause() != null) {
      TagFilter tagFilter = parseWithClause(ctx.withClause());
      deleteColumnsStatement.setTagFilter(tagFilter);
    }
    return deleteColumnsStatement;
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
      String ip =
          ipStr.substring(
              ipStr.indexOf(SQLConstant.QUOTE) + 1, ipStr.lastIndexOf(SQLConstant.QUOTE));
      int port = Integer.parseInt(engine.port.getText());
      String typeStr = engine.engineType.getText().trim();
      String type =
          typeStr.substring(
              typeStr.indexOf(SQLConstant.QUOTE) + 1, typeStr.lastIndexOf(SQLConstant.QUOTE));
      Map<String, String> extra = parseExtra(engine.extra);
      addStorageEngineStatement.setEngines(
          new StorageEngine(ip, port, StorageEngineType.valueOf(type.toLowerCase()), extra));
    }
    return addStorageEngineStatement;
  }

  @Override
  public Statement visitShowColumnsStatement(ShowColumnsStatementContext ctx) {
    return parseShowColumnsOptions(ctx.showColumnsOptions());
  }

  private ShowColumnsStatement parseShowColumnsOptions(ShowColumnsOptionsContext ctx) {
    ShowColumnsStatement showColumnsStatement = new ShowColumnsStatement();
    if (ctx != null) {
      for (PathContext pathRegex : ctx.path()) {
        showColumnsStatement.setPathRegex(parsePath(pathRegex));
      }
      if (ctx.withClause() != null) {
        TagFilter tagFilter = parseWithClause(ctx.withClause());
        showColumnsStatement.setTagFilter(tagFilter);
      }
      if (ctx.limitClause() != null) {
        parseLimitClause(ctx.limitClause(), showColumnsStatement);
      }
    }
    return showColumnsStatement;
  }

  @Override
  public Statement visitShowClusterInfoStatement(ShowClusterInfoStatementContext ctx) {
    return new ShowClusterInfoStatement();
  }

  @Override
  public Statement visitRemoveHistoryDataSourceStatement(
      RemoveHistoryDataSourceStatementContext ctx) {
    RemoveHistoryDataSourceStatement statement = new RemoveHistoryDataSourceStatement();
    ctx.removedStorageEngine()
        .forEach(
            storageEngine -> {
              String ipStr = storageEngine.ip.getText();
              String schemaPrefixStr = storageEngine.schemaPrefix.getText();
              String dataPrefixStr = storageEngine.dataPrefix.getText();
              String ip =
                  ipStr.substring(
                      ipStr.indexOf(SQLConstant.QUOTE) + 1, ipStr.lastIndexOf(SQLConstant.QUOTE));
              String schemaPrefix =
                  schemaPrefixStr.substring(
                      schemaPrefixStr.indexOf(SQLConstant.QUOTE) + 1,
                      schemaPrefixStr.lastIndexOf(SQLConstant.QUOTE));
              String dataPrefix =
                  dataPrefixStr.substring(
                      dataPrefixStr.indexOf(SQLConstant.QUOTE) + 1,
                      dataPrefixStr.lastIndexOf(SQLConstant.QUOTE));
              statement.addStorageEngine(
                  new RemovedStorageEngineInfo(
                      ip,
                      Integer.parseInt(storageEngine.port.getText()),
                      schemaPrefix,
                      dataPrefix));
            });
    return statement;
  }

  private void parseFromParts(FromClauseContext ctx, UnarySelectStatement selectStatement) {
    List<FromPart> fromParts = new ArrayList<>();
    // parse first FromPart
    fromParts.add(parseTableReference(ctx.tableReference(), selectStatement.getCteList()));
    // parse joined FromPart
    if (ctx.joinPart() != null && !ctx.joinPart().isEmpty()) {
      selectStatement.setHasJoinParts(true);
      for (JoinPartContext joinPartContext : ctx.joinPart()) {
        FromPart joinPart =
            parseTableReference(joinPartContext.tableReference(), selectStatement.getCteList());
        // cross join
        if (joinPartContext.join() == null) {
          joinPart.setJoinCondition(new JoinCondition());
          fromParts.add(joinPart);
          continue;
        }
        // inner join and outer join
        JoinType joinType = parseJoinType(joinPartContext.join());
        Filter filter = null;
        if (joinPartContext.orExpression() != null) {
          filter = parseOrExpression(joinPartContext.orExpression(), selectStatement).getFilter();
        }
        List<String> columns = new ArrayList<>();
        if (joinPartContext.colList() != null && !joinPartContext.colList().isEmpty()) {
          joinPartContext
              .colList()
              .path()
              .forEach(pathContext -> columns.add(parsePath(pathContext)));
        }
        joinPart.setJoinCondition(new JoinCondition(joinType, filter, columns));
        fromParts.add(joinPart);
      }
    }
    checkShowColumnsRename(fromParts);
    selectStatement.setFromParts(fromParts);
  }

  private FromPart parseTableReference(
      TableReferenceContext ctx, List<CommonTableExpression> cteList) {
    if (ctx.path() != null) {
      String fromPath = parsePath(ctx.path());
      for (CommonTableExpression cte : cteList) {
        if (cte.getName().equals(fromPath)) {
          return ctx.asClause() != null
              ? new CteFromPart(cte, ctx.asClause().ID().getText())
              : new CteFromPart(cte);
        }
      }
      return ctx.asClause() != null
          ? new PathFromPart(fromPath, ctx.asClause().ID().getText())
          : new PathFromPart(fromPath);
    } else if (ctx.subquery() != null) {
      SelectStatement subStatement = parseQueryClause(ctx.subquery().queryClause(), cteList, true);
      if (ctx.subquery().orderByClause() != null) {
        parseOrderByClause(ctx.subquery().orderByClause(), subStatement);
      }
      if (ctx.subquery().limitClause() != null) {
        parseLimitClause(ctx.subquery().limitClause(), subStatement);
      }
      // 计算子查询的自由变量
      subStatement.initFreeVariables();
      return ctx.asClause() != null
          ? new SubQueryFromPart(subStatement, ctx.asClause().ID().getText())
          : new SubQueryFromPart(subStatement);
    } else {
      ShowColumnsStatement statement = parseShowColumnsOptions(ctx.showColumnsOptions());
      return ctx.asClause() != null
          ? new ShowColumnsFromPart(statement, ctx.asClause().ID().getText())
          : new ShowColumnsFromPart(statement);
    }
  }

  private void checkShowColumnsRename(List<FromPart> fromParts) {
    if (fromParts.size() < 2) {
      return;
    }
    int num = 0;
    for (FromPart fromPart : fromParts) {
      if (fromPart.getType().equals(FromPartType.ShowColumns) && !fromPart.hasAlias()) {
        num++;
      }
      if (num >= 2) {
        throw new SQLParserException(
            "As clause is expected when multiple ShowColumns are joined together.");
      }
    }
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

    List<UDFClassPair> classPairs = new ArrayList<>();
    ctx.udfClassRef()
        .forEach(
            e -> {
              UDFClassPair p =
                  new UDFClassPair(e.name.getText().trim(), e.className.getText().trim());
              p.name = p.name.substring(1, p.name.length() - 1).trim();
              p.classPath = p.classPath.substring(1, p.classPath.length() - 1).trim();
              classPairs.add(p);
            });

    List<UDFType> types = new ArrayList<>();
    ctx.udfType()
        .forEach(
            e -> {
              switch (e.getText().trim().toLowerCase()) {
                case "udsf":
                  types.add(UDFType.UDSF);
                  break;
                case "udtf":
                  types.add(UDFType.UDTF);
                  break;
                case "udaf":
                  types.add(UDFType.UDAF);
                  break;
                default:
                  types.add(UDFType.TRANSFORM);
              }
            });
    return new RegisterTaskStatement(filePath, classPairs, types);
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

  @Override
  public Statement visitSetConfigStatement(SetConfigStatementContext ctx) {
    String name = ctx.configName.getText();
    name = name.substring(1, name.length() - 1);
    String value = ctx.configValue.getText();
    value = value.substring(1, value.length() - 1);
    return new SetConfigStatement(name, value);
  }

  @Override
  public Statement visitShowConfigStatement(ShowConfigStatementContext ctx) {
    String configName;
    if (ctx.configName == null) {
      configName = "*";
    } else {
      configName = ctx.configName.getText();
      configName = configName.substring(1, configName.length() - 1);
    }
    return new ShowConfigStatement(configName);
  }

  @Override
  public Statement visitShowSessionIDStatement(ShowSessionIDStatementContext ctx) {
    return new ShowSessionIDStatement();
  }

  @Override
  public Statement visitSetRulesStatement(SetRulesStatementContext ctx) {
    Map<String, Boolean> rulesChange = new HashMap<>();

    ctx.ruleAssignment()
        .forEach(
            ruleAssignmentContext -> {
              String ruleName = ruleAssignmentContext.ruleName.getText();
              boolean enable = ruleAssignmentContext.ruleValue.getText().equalsIgnoreCase("on");
              rulesChange.put(ruleName, enable);
            });
    return new SetRulesStatement(rulesChange);
  }

  @Override
  public Statement visitShowRulesStatement(ShowRulesStatementContext ctx) {
    return new ShowRulesStatement();
  }

  private void parseSelectPaths(SelectClauseContext ctx, UnarySelectStatement selectStatement) {
    if (ctx.VALUE2META() != null) {
      parseSelectPathsWithValue2Meta(ctx, selectStatement);
      return;
    }

    if (ctx.DISTINCT() != null) {
      selectStatement.setDistinct(true);
    }

    List<SelectSublistContext> selectList = ctx.selectSublist();

    for (SelectSublistContext select : selectList) {
      List<Expression> ret = parseExpression(select.expression(), selectStatement);
      if (select.expression().subquery() != null && select.asClause() != null) {
        throw new SQLParserException("Select Subquery doesn't support AS clause.");
      }
      if (ret.size() == 1 && select.asClause() != null) {
        ret.get(0).setAlias(select.asClause().ID().getText());
      }
      ret.forEach(
          expression -> {
            if (ExpressionUtils.isConstantArithmeticExpr(expression)) {
              throw new SQLParserException(
                  "SELECT constant arithmetic expression isn't supported yet.");
            } else {
              selectStatement.addSelectClauseExpression(expression);
            }
          });
    }

    if (!selectStatement.getFuncTypeSet().isEmpty()) {
      selectStatement.setHasFunc(true);
    }
  }

  private void parseSelectPathsWithValue2Meta(
      SelectClauseContext ctx, UnarySelectStatement selectStatement) {
    SelectStatement subStatement =
        parseQueryClause(ctx.queryClause(), selectStatement.getCteList(), false);
    if (ctx.orderByClause() != null) {
      parseOrderByClause(ctx.orderByClause(), subStatement);
    }
    if (ctx.limitClause() != null) {
      parseLimitClause(ctx.limitClause(), subStatement);
    }

    selectStatement.addSelectClauseExpression(new FromValueExpression(subStatement));
    selectStatement.setHasValueToSelectedPath(true);
  }

  private List<Expression> parseExpression(
      ExpressionContext ctx, UnarySelectStatement selectStatement) {
    return parseExpression(ctx, selectStatement, true);
  }

  private List<Expression> parseExpression(
      ExpressionContext ctx, UnarySelectStatement selectStatement, boolean isFromSelectClause) {
    if (ctx.function() != null) {
      return Collections.singletonList(parseFuncExpression(ctx, selectStatement));
    }
    if (ctx.path() != null && !ctx.path().isEmpty()) {
      return Collections.singletonList(
          parseBaseExpression(ctx, selectStatement, isFromSelectClause));
    }
    if (ctx.constant() != null) {
      return Collections.singletonList(new ConstantExpression(parseValue(ctx.constant())));
    }

    List<Expression> ret = new ArrayList<>();
    if (ctx.inBracketExpr != null) {
      List<Expression> expressions =
          parseExpression(ctx.inBracketExpr, selectStatement, isFromSelectClause);
      for (Expression expression : expressions) {
        ret.add(new BracketExpression(expression));
      }
    } else if (ctx.expr != null) {
      List<Expression> expressions = parseExpression(ctx.expr, selectStatement, isFromSelectClause);
      Operator operator = parseOperator(ctx);
      for (Expression expression : expressions) {
        ret.add(new UnaryExpression(operator, expression));
      }
    } else if (ctx.leftExpr != null && ctx.rightExpr != null) {
      List<Expression> leftExpressions =
          parseExpression(ctx.leftExpr, selectStatement, isFromSelectClause);
      List<Expression> rightExpressions =
          parseExpression(ctx.rightExpr, selectStatement, isFromSelectClause);
      Operator operator = parseOperator(ctx);
      for (Expression leftExpression : leftExpressions) {
        for (Expression rightExpression : rightExpressions) {
          ret.add(new BinaryExpression(leftExpression, rightExpression, operator));
        }
      }
    } else if (ctx.subquery() != null) {
      SelectStatement subStatement =
          parseQueryClause(ctx.subquery().queryClause(), selectStatement.getCteList(), true);
      if (ctx.subquery().orderByClause() != null) {
        throw new SQLParserException("Not supported for order by used in select subquery.");
      }
      if (ctx.subquery().limitClause() != null) {
        throw new SQLParserException("Not supported for limit used in select subquery.");
      }
      if (subStatement.getSelectType() != SelectStatement.SelectStatementType.UNARY) {
        throw new SQLParserException("Not supported for set operator used in select subquery.");
      }
      UnarySelectStatement subUnaryStatement = (UnarySelectStatement) subStatement;
      // 计算子查询的自由变量
      subUnaryStatement.initFreeVariables();

      Filter filter = new BoolFilter(true);

      selectStatement.addSelectSubQueryPart(
          new SubQueryFromPart(subUnaryStatement, new JoinCondition(JoinType.SingleJoin, filter)));
      subUnaryStatement
          .getExpressions()
          .forEach(
              expression -> {
                String selectedPath;
                if (expression.hasAlias()) {
                  selectedPath = expression.getAlias();
                } else {
                  selectedPath = expression.getColumnName();
                }
                BaseExpression baseExpression = new BaseExpression(selectedPath);
                ret.add(baseExpression);
              });
    } else {
      throw new SQLParserException("Illegal selected expression");
    }
    return ret;
  }

  private Expression parseFuncExpression(
      ExpressionContext ctx, UnarySelectStatement selectStatement) {
    FunctionContext funcCtx = ctx.function();
    String funcName = funcCtx.functionName().getText();

    boolean isDistinct = false;
    if (funcCtx.ALL() != null || funcCtx.DISTINCT() != null) {
      if (!isCanUseSetQuantifierFunction(funcName)) {
        throw new SQLParserException(
            "Function: " + funcName + " can't use ALL or DISTINCT in bracket.");
      }
      if (funcCtx.DISTINCT() != null) {
        isDistinct = true;
      }
    }

    List<String> columns = new ArrayList<>();
    for (PathContext pathContext : funcCtx.path()) {
      columns.add(parsePath(pathContext));
    }

    List<Object> args = new ArrayList<>();
    Map<String, Object> kvargs = new HashMap<>();
    for (ParamContext paramContext : funcCtx.param()) {
      Object val = parseValue(paramContext.value);
      if (paramContext.key != null) {
        String key = paramContext.key.getText();
        kvargs.put(key, val);
      } else {
        args.add(val);
      }
    }

    // 如果查询语句中FROM子句只有一个部分且FROM一个前缀，则SELECT子句中的path只用写出后缀
    if (selectStatement.isFromSinglePath()) {
      String fromPath = selectStatement.getFromPart(0).getPrefix();

      List<String> newColumns = new ArrayList<>();
      for (String column : columns) {
        newColumns.add(fromPath + SQLConstant.DOT + column);
      }
      columns = newColumns;
    }
    FuncExpression expression = new FuncExpression(funcName, columns, args, kvargs, isDistinct);
    selectStatement.setSelectedFuncsAndExpression(funcName, expression);
    return expression;
  }

  private Expression parseBaseExpression(
      ExpressionContext ctx, UnarySelectStatement selectStatement, boolean isFromSelectClause) {
    String selectedPath = parsePath(ctx.path());
    // 如果查询语句中FROM子句只有一个部分且FROM一个前缀，则SELECT子句中的path只用写出后缀
    if (selectStatement.isFromSinglePath()) {
      FromPart fromPart = selectStatement.getFromPart(0);
      String fullPath = fromPart.getPrefix() + SQLConstant.DOT + selectedPath;
      String originFullPath = fromPart.getOriginPrefix() + SQLConstant.DOT + selectedPath;
      BaseExpression expression = new BaseExpression(fullPath);
      if (isFromSelectClause) {
        selectStatement.addSelectPath(originFullPath);
      }
      return expression;
    } else {
      BaseExpression expression = new BaseExpression(selectedPath);
      if (isFromSelectClause) {
        selectStatement.addSelectPath(selectedPath);
      }
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

  private void parseSpecialClause(SpecialClauseContext ctx, UnarySelectStatement selectStatement) {
    if (ctx.downsampleClause() != null) {
      parseDownsampleClause(ctx.downsampleClause(), selectStatement);
    }
    if (ctx.groupByClause() != null) {
      parseGroupByClause(ctx.groupByClause(), selectStatement);
    }
    if (ctx.havingClause() != null) {
      FilterData filterData = parseOrExpression(ctx.havingClause().orExpression(), selectStatement);
      selectStatement.setHavingFilter(filterData.getFilter());
      filterData.getPathList().forEach(selectStatement::addHavingPath);
      filterData.getSubQueryFromPartList().forEach(selectStatement::addHavingSubQueryPart);
    }
  }

  private void parseDownsampleClause(
      DownsampleClauseContext ctx, UnarySelectStatement selectStatement) {
    long precision = parseAggLen(ctx.aggLen(0));
    Pair<Long, Long> timeInterval =
        ctx.timeInterval() == null ? null : parseTimeInterval(ctx.timeInterval());
    long startKey = timeInterval == null ? KEY_MIN_VAL : timeInterval.k;
    long endKey = timeInterval == null ? KEY_MAX_VAL : timeInterval.v;
    selectStatement.setStartKey(startKey);
    selectStatement.setEndKey(endKey);
    selectStatement.setPrecision(precision);
    selectStatement.setSlideDistance(precision);
    selectStatement.setHasDownsample(true);
    if (ctx.SLIDE() != null) {
      long distance = parseAggLen(ctx.aggLen(1));
      selectStatement.setSlideDistance(distance);
    }

    // merge value filter and group time range filter
    KeyFilter startKeyFilter = new KeyFilter(Op.GE, startKey);
    KeyFilter endKeyFilter = new KeyFilter(Op.L, endKey);
    Filter mergedFilter;
    if (selectStatement.hasValueFilter()) {
      mergedFilter =
          new AndFilter(
              new ArrayList<>(
                  Arrays.asList(selectStatement.getFilter(), startKeyFilter, endKeyFilter)));
    } else {
      mergedFilter = new AndFilter(new ArrayList<>(Arrays.asList(startKeyFilter, endKeyFilter)));
      selectStatement.setHasValueFilter(true);
    }
    selectStatement.setFilter(mergedFilter);
  }

  private void parseGroupByClause(GroupByClauseContext ctx, UnarySelectStatement selectStatement) {
    selectStatement.setHasGroupBy(true);

    ctx.path()
        .forEach(
            pathContext -> {
              String path, originPath;
              // 如果查询语句的FROM子句只有一个部分且FROM一个前缀，则GROUP BY后的path只用写出后缀
              if (selectStatement.isFromSinglePath()) {
                FromPart fromPart = selectStatement.getFromPart(0);
                path = fromPart.getPrefix() + SQLConstant.DOT + parsePath(pathContext);
                originPath = fromPart.getOriginPrefix() + SQLConstant.DOT + parsePath(pathContext);
              } else {
                path = parsePath(pathContext);
                originPath = parsePath(pathContext);
              }
              if (path.contains("*")) {
                throw new SQLParserException(
                    String.format("GROUP BY path '%s' has '*', which is not supported.", path));
              }
              selectStatement.setGroupByPath(path);
              selectStatement.addGroupByPath(originPath);
            });

    selectStatement
        .getBaseExpressionList()
        .forEach(
            expr -> {
              if (!selectStatement.getGroupByPaths().contains(expr.getPathName())) {
                throw new SQLParserException("Selected path must exist in group by clause.");
              }
            });
  }

  // like standard SQL, limit N, M means limit M offset N
  private void parseLimitClause(LimitClauseContext ctx, SelectStatement selectStatement) {
    Pair<Integer, Integer> p = getLimitAndOffsetFromCtx(ctx);
    selectStatement.setLimit(p.k);
    selectStatement.setOffset(p.v);
  }

  private void parseLimitClause(LimitClauseContext ctx, ShowColumnsStatement statement) {
    Pair<Integer, Integer> p = getLimitAndOffsetFromCtx(ctx);
    statement.setLimit(p.k);
    statement.setOffset(p.v);
  }

  private Pair<Integer, Integer> getLimitAndOffsetFromCtx(LimitClauseContext ctx)
      throws SQLParserException {
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
        String suffix = parsePath(pathContext);
        String orderByPath = suffix;
        if (selectStatement.getSelectType() == SelectStatement.SelectStatementType.UNARY) {
          UnarySelectStatement unarySelectStatement = (UnarySelectStatement) selectStatement;
          String prefix = unarySelectStatement.getFromPart(0).getPrefix();

          // 如果查询语句的FROM子句只有一个部分且FROM一个前缀，则ORDER BY后的path只用写出后缀
          if (unarySelectStatement.isFromSinglePath()) {
            orderByPath = prefix + SQLConstant.DOT + suffix;
          }
        }
        if (orderByPath.contains("*")) {
          throw new SQLParserException(
              String.format("ORDER BY path '%s' has '*', which is not supported.", orderByPath));
        }
        selectStatement.setOrderByPath(orderByPath);
      }
    }
    if (ctx.DESC() != null) {
      selectStatement.setAscending(false);
    }
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
    long startKey, endKey;

    if (interval == null) {
      startKey = 0;
      endKey = Long.MAX_VALUE;
    } else {
      // use index +- 1 to implement [start, end], [start, end),
      // (start, end), (start, end] range in [start, end) interface.
      if (interval.LR_BRACKET() != null) { // (
        startKey = parseTime(interval.startTime) + 1;
      } else {
        startKey = parseTime(interval.startTime);
      }

      if (interval.RR_BRACKET() != null) { // )
        endKey = parseTime(interval.endTime);
      } else {
        endKey = parseTime(interval.endTime) + 1;
      }
    }

    if (startKey > endKey) {
      throw new SQLParserException("start key should be smaller than end key in key interval.");
    }

    return new Pair<>(startKey, endKey);
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

  private FilterData parseOrExpression(OrExpressionContext ctx, Statement statement) {
    List<FilterData> children = new ArrayList<>();
    for (AndExpressionContext andCtx : ctx.andExpression()) {
      children.add(parseAndExpression(andCtx, statement));
    }
    return children.size() == 1 ? children.get(0) : new FilterData(children, FilterType.Or);
  }

  private FilterData parseAndExpression(AndExpressionContext ctx, Statement statement) {
    List<FilterData> children = new ArrayList<>();
    for (PredicateContext predicateCtx : ctx.predicate()) {
      children.add(parsePredicate(predicateCtx, statement));
    }
    return children.size() == 1 ? children.get(0) : new FilterData(children, FilterType.And);
  }

  private FilterData parsePredicate(PredicateContext ctx, Statement statement) {
    if (ctx.orExpression() != null) {
      FilterData filterData = parseOrExpression(ctx.orExpression(), statement);
      if (ctx.OPERATOR_NOT() != null) {
        filterData.setFilter(new NotFilter(filterData.getFilter()));
      }
      return filterData;
    } else if (ctx.path().isEmpty()
        && ctx.predicateWithSubquery() == null
        && ctx.constant() != null) {
      return parseKeyFilter(ctx);
    } else {
      StatementType type = statement.getType();
      if (type != StatementType.SELECT) {
        throw new SQLParserException(
            String.format(
                "%s clause can not use value or path filter.", type.toString().toLowerCase()));
      }

      if (ctx.predicateWithSubquery() != null) {
        return parseFilterWithSubQuery(
            ctx.predicateWithSubquery(), (UnarySelectStatement) statement);
      } else if (ctx.expression().size() == 2) {
        return parseExprFilter(ctx, (UnarySelectStatement) statement);
      } else if (ctx.path().size() == 1) {
        return parseValueFilter(ctx, (UnarySelectStatement) statement);
      } else {
        return parsePathFilter(ctx, (UnarySelectStatement) statement);
      }
    }
  }

  private FilterData parseKeyFilter(PredicateContext ctx) {
    Op op = Op.str2Op(ctx.comparisonOperator().getText());
    // deal with sub clause like 100 < key
    if (ctx.children.get(0) instanceof ConstantContext) {
      op = Op.getDirectionOpposite(op);
    }
    long time = (long) parseValue(ctx.constant());
    return new FilterData(new KeyFilter(op, time));
  }

  private FilterData parseExprFilter(PredicateContext ctx, UnarySelectStatement statement) {
    Op op = Op.str2Op(ctx.comparisonOperator().getText());
    assert ctx.expression().size() == 2;
    Expression expressionA = parseExpression(ctx.expression().get(0), statement, false).get(0);
    Expression expressionB = parseExpression(ctx.expression().get(1), statement, false).get(0);
    return new FilterData(new ExprFilter(expressionA, op, expressionB));
  }

  private FilterData parseValueFilter(PredicateContext ctx, UnarySelectStatement statement) {
    String path = parsePath(ctx.path().get(0));

    if (statement.isFromSinglePath() && !statement.isSubQuery()) {
      FromPart fromPart = statement.getFromPart(0);
      path = fromPart.getPrefix() + SQLConstant.DOT + path;
    }

    // deal with having filter with functions like having avg(a) > 3.
    // we need a instead of avg(a) to combine fragments' raw data.
    if (ctx.functionName() != null) {
      path = ctx.functionName().getText() + "(" + path + ")";
    }

    Op op;
    if (ctx.stringLikeOperator() != null) {
      op = Op.str2Op(ctx.stringLikeOperator().getText().trim().toLowerCase());
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

    FilterData filterData = new FilterData(new ValueFilter(path, op, value));
    filterData.addPaths(getPathsFromPredicate(ctx, statement));

    return filterData;
  }

  private FilterData parsePathFilter(PredicateContext ctx, UnarySelectStatement statement) {
    String pathA = parsePath(ctx.path().get(0));
    String pathB = parsePath(ctx.path().get(1));

    Op op = Op.str2Op(ctx.comparisonOperator().getText().trim().toLowerCase());

    // 如果查询语句不是一个子查询，FROM子句只有一个部分且FROM一个前缀，则WHERE条件中的path只用写出后缀
    if (statement.isFromSinglePath() && !statement.isSubQuery()) {
      FromPart fromPart = statement.getFromPart(0);
      pathA = fromPart.getPrefix() + SQLConstant.DOT + pathA;
      pathB = fromPart.getPrefix() + SQLConstant.DOT + pathB;
    }

    FilterData filterData = new FilterData(new PathFilter(pathA, op, pathB));
    filterData.addPaths(getPathsFromPredicate(ctx, statement));
    return filterData;
  }

  private FilterData parseFilterWithSubQuery(
      PredicateWithSubqueryContext ctx, UnarySelectStatement statement) {
    if (ctx.EXISTS() != null) {
      return parseExistsFilter(ctx, statement);
    } else if (ctx.IN() != null) {
      return parseInFilter(ctx, statement);
    } else if (ctx.quantifier() != null) {
      return parseQuantifierComparisonFilter(ctx, statement);
    } else if (ctx.subquery().size() == 1) {
      return parseScalarSubQueryComparisonFilter(ctx, statement);
    } else {
      return parseTwoScalarSubQueryComparisonFilter(ctx, statement);
    }
  }

  private FilterData parseExistsFilter(
      PredicateWithSubqueryContext ctx, UnarySelectStatement statement) {
    SelectStatement subStatement = buildSubStatement(ctx, statement, 0, -1);

    // 计算子查询的自由变量
    subStatement.initFreeVariables();
    String markColumn = MARK_PREFIX + markJoinCount;
    markJoinCount += 1;

    Filter filter = new BoolFilter(true);

    boolean isAntiJoin = ctx.OPERATOR_NOT() != null;
    SubQueryFromPart subQueryPart =
        new SubQueryFromPart(
            subStatement, new JoinCondition(JoinType.MarkJoin, filter, markColumn, isAntiJoin));

    FilterData filterData = new FilterData(new ValueFilter(markColumn, Op.E, new Value(true)));
    filterData.addSubQueryFromPart(subQueryPart);
    return filterData;
  }

  private FilterData parseInFilter(
      PredicateWithSubqueryContext ctx, UnarySelectStatement statement) {
    SelectStatement subStatement = buildSubStatement(ctx, statement, 0, 1);
    // 计算子查询的自由变量
    subStatement.initFreeVariables();
    String markColumn = MARK_PREFIX + markJoinCount;
    markJoinCount += 1;

    Filter filter;
    Expression expression = subStatement.getExpressions().get(0);
    if (ctx.constant() != null) {
      Value value = new Value(parseValue(ctx.constant()));
      String path = expression.hasAlias() ? expression.getAlias() : expression.getColumnName();
      filter = new ValueFilter(path, Op.E, value);
    } else {
      String pathA = parsePath(ctx.path());
      if (statement.isFromSinglePath() && !statement.isSubQuery()) {
        pathA = statement.getFromPart(0).getPrefix() + SQLConstant.DOT + pathA;
      }
      // deal with having filter with functions
      if (ctx.functionName() != null) {
        pathA = ctx.functionName().getText() + "(" + pathA + ")";
      }

      String pathB = expression.hasAlias() ? expression.getAlias() : expression.getColumnName();
      filter = new PathFilter(pathA, Op.E, pathB);
      subStatement.addFreeVariable(pathA);
    }

    boolean isAntiJoin = ctx.OPERATOR_NOT() != null;
    SubQueryFromPart subQueryPart =
        new SubQueryFromPart(
            subStatement, new JoinCondition(JoinType.MarkJoin, filter, markColumn, isAntiJoin));

    FilterData filterData = new FilterData(new ValueFilter(markColumn, Op.E, new Value(true)));
    filterData.addSubQueryFromPart(subQueryPart);
    return filterData;
  }

  private FilterData parseQuantifierComparisonFilter(
      PredicateWithSubqueryContext ctx, UnarySelectStatement statement) {
    SelectStatement subStatement = buildSubStatement(ctx, statement, 0, 1);
    // 计算子查询的自由变量
    subStatement.initFreeVariables();
    String markColumn = MARK_PREFIX + markJoinCount;
    markJoinCount += 1;

    Filter filter;
    Op op = Op.str2Op(ctx.comparisonOperator().getText().trim().toLowerCase());

    Expression expression = subStatement.getExpressions().get(0);
    if (ctx.constant() != null) {
      Value value = new Value(parseValue(ctx.constant()));
      String path = expression.hasAlias() ? expression.getAlias() : expression.getColumnName();

      if (ctx.quantifier().all() != null) {
        op = path.contains("*") ? Op.getDeMorganOpposite(op) : Op.getOpposite(op);
      }

      filter = new ValueFilter(path, op, value);
    } else {
      String pathA = parsePath(ctx.path());
      if (statement.isFromSinglePath() && !statement.isSubQuery()) {
        pathA = statement.getFromPart(0).getPrefix() + SQLConstant.DOT + pathA;
      }
      // deal with having filter with functions
      if (ctx.functionName() != null) {
        pathA = ctx.functionName().getText() + "(" + pathA + ")";
      }

      String pathB = expression.hasAlias() ? expression.getAlias() : expression.getColumnName();

      if (ctx.quantifier().all() != null) {
        op = Op.getOpposite(op);
      }

      filter = new PathFilter(pathA, op, pathB);
      subStatement.addFreeVariable(pathA);
    }

    boolean isAntiJoin = ctx.quantifier().all() != null;
    SubQueryFromPart subQueryPart =
        new SubQueryFromPart(
            subStatement, new JoinCondition(JoinType.MarkJoin, filter, markColumn, isAntiJoin));

    FilterData filterData = new FilterData(new ValueFilter(markColumn, Op.E, new Value(true)));
    filterData.addSubQueryFromPart(subQueryPart);
    return filterData;
  }

  private FilterData parseScalarSubQueryComparisonFilter(
      PredicateWithSubqueryContext ctx, UnarySelectStatement statement) {
    SelectStatement subStatement = buildSubStatement(ctx, statement, 0, 1);
    // 计算子查询的自由变量
    subStatement.initFreeVariables();

    Filter filter = new BoolFilter(true);

    SubQueryFromPart subQueryPart =
        new SubQueryFromPart(subStatement, new JoinCondition(JoinType.SingleJoin, filter));

    FilterData filterData = new FilterData();
    filterData.addSubQueryFromPart(subQueryPart);

    Expression expression = subStatement.getExpressions().get(0);
    Op op = Op.str2Op(ctx.comparisonOperator().getText().trim().toLowerCase());
    if (ctx.constant() != null) {
      Value value = new Value(parseValue(ctx.constant()));
      String path = expression.hasAlias() ? expression.getAlias() : expression.getColumnName();
      filterData.setFilter(new ValueFilter(path, op, value));
      return filterData;
    } else {
      String pathA = parsePath(ctx.path());
      if (statement.isFromSinglePath() && !statement.isSubQuery()) {
        pathA = statement.getFromPart(0).getPrefix() + SQLConstant.DOT + pathA;
      }
      // deal with having filter with functions
      if (ctx.functionName() != null) {
        pathA = ctx.functionName().getText() + "(" + pathA + ")";
      }

      String pathB = expression.hasAlias() ? expression.getAlias() : expression.getColumnName();
      filterData.setFilter(new PathFilter(pathA, op, pathB));
      return filterData;
    }
  }

  private FilterData parseTwoScalarSubQueryComparisonFilter(
      PredicateWithSubqueryContext ctx, UnarySelectStatement statement) {
    List<String> paths = new ArrayList<>();
    FilterData filterData = new FilterData();
    for (int i = 0; i < 2; i++) {
      SelectStatement subStatement = buildSubStatement(ctx, statement, i, 1);
      // 计算子查询的自由变量
      subStatement.initFreeVariables();
      Expression expression = subStatement.getExpressions().get(0);
      paths.add(expression.hasAlias() ? expression.getAlias() : expression.getColumnName());

      Filter filter = new BoolFilter(true);

      SubQueryFromPart subQueryPart =
          new SubQueryFromPart(subStatement, new JoinCondition(JoinType.SingleJoin, filter));
      filterData.addSubQueryFromPart(subQueryPart);
    }

    Op op = Op.str2Op(ctx.comparisonOperator().getText().trim().toLowerCase());

    filterData.setFilter(new PathFilter(paths.get(0), op, paths.get(1)));
    return filterData;
  }

  /**
   * 根据PredicateWithSubqueryContext构建subStatement
   *
   * @param ctx 子查询的上下文
   * @param statement 外层的statement
   * @param index 子查询的索引
   * @param expressionSize 外层表达式的大小，用于判断子查询的列数是否正确（-1表示不判断）
   * @return 构造好的子查询SelectStatement
   */
  private SelectStatement buildSubStatement(
      PredicateWithSubqueryContext ctx,
      UnarySelectStatement statement,
      int index,
      int expressionSize) {
    SelectStatement subStatement =
        parseQueryClause(ctx.subquery().get(index).queryClause(), statement.getCteList(), true);

    if (ctx.subquery().get(0).orderByClause() != null
        && ctx.subquery().get(0).limitClause() != null) {
      parseOrderByClause(ctx.subquery().get(0).orderByClause(), subStatement);
      parseLimitClause(ctx.subquery().get(0).limitClause(), subStatement);
    } else if (ctx.subquery().get(0).orderByClause() != null
        ^ ctx.subquery().get(0).limitClause() != null) {
      throw new SQLParserException(
          "order by and limit should be used at the same time in where subquery");
    }
    if (expressionSize >= 0 && subStatement.getExpressions().size() != expressionSize) {
      throw new SQLParserException(
          "The number of columns in sub-query doesn't equal to outer row.");
    }
    return subStatement;
  }

  private Map<String, String> parseExtra(StringLiteralContext ctx) {
    Map<String, String> map = new HashMap<>();
    String extra = ctx.getText().trim();
    if (extra.length() == 0 || extra.equals(SQLConstant.DOUBLE_QUOTES)) {
      return map;
    }
    extra =
        extra.substring(extra.indexOf(SQLConstant.QUOTE) + 1, extra.lastIndexOf(SQLConstant.QUOTE));
    String[] kvStr = extra.split(SQLConstant.COMMA);
    for (String kv : kvStr) {
      String[] kvArray = kv.split(SQLConstant.COLON);
      if (kvArray.length != 2) {
        if (kv.contains("url")) {
          map.put("url", kv.substring(kv.indexOf(":") + 1));
        } else if (kv.contains("dir")) {
          // for windows absolute path
          String dirType = kv.substring(0, kv.indexOf(":")).trim();
          String dirPath = kv.substring(kv.indexOf(":") + 1).trim();
          map.put(dirType, dirPath);
        }
        continue;
      }
      map.put(kvArray[0].trim(), kvArray[1].trim());
    }
    return map;
  }

  private void parseInsertValuesSpec(InsertValuesSpecContext ctx, InsertStatement insertStatement) {
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

    insertStatement.setKeys(new ArrayList<>(Arrays.asList(times)));
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
      throw new SQLParserException(String.format("Input time format %s error. ", ctx.getText()));
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

  private List<String> getPathsFromPredicate(
      PredicateContext predicateContext, UnarySelectStatement statement) {
    List<String> paths = new ArrayList<>();
    for (int i = 0; i < 2 && i < predicateContext.path().size(); i++) {
      String path = parsePath(predicateContext.path().get(i));
      String originPath = path;
      // 如果查询语句不是一个子查询，FROM子句只有一个部分且FROM一个前缀，则WHERE条件中的path只用写出后缀
      if (statement.isFromSinglePath() && !statement.isSubQuery()) {
        FromPart fromPart = statement.getFromPart(0);
        path = fromPart.getPrefix() + SQLConstant.DOT + path;
        originPath = fromPart.getOriginPrefix() + SQLConstant.DOT + originPath;
      }
      if (!statement.isFreeVariable(path)) {
        paths.add(originPath);
      }
    }
    return paths;
  }

  private static class FilterData {
    private Filter filter;
    private List<String> pathList;

    private List<SubQueryFromPart> subQueryFromPartList;

    public FilterData() {
      this.filter = null;
      this.pathList = new ArrayList<>();
      this.subQueryFromPartList = new ArrayList<>();
    }

    public FilterData(Filter filter) {
      this.filter = filter;
      this.pathList = new ArrayList<>();
      this.subQueryFromPartList = new ArrayList<>();
    }

    public FilterData(List<FilterData> filterDataList, FilterType filterType) {
      this.pathList = new ArrayList<>();
      this.subQueryFromPartList = new ArrayList<>();
      addAll(filterDataList, filterType);
    }

    public void addAll(List<FilterData> filterDataList, FilterType filterType) {
      List<Filter> filterList =
          filterDataList.stream().map(FilterData::getFilter).collect(Collectors.toList());
      for (FilterData filterData : filterDataList) {
        this.pathList.addAll(filterData.getPathList());
        this.subQueryFromPartList.addAll(filterData.getSubQueryFromPartList());
      }

      if (filterType == FilterType.And) {
        this.filter = new AndFilter(filterList);
      } else if (filterType == FilterType.Or) {
        this.filter = new OrFilter(filterList);
      } else {
        throw new SQLParserException("Filter must be combined by And or Or.");
      }
    }

    public void setFilter(Filter filter) {
      this.filter = filter;
    }

    public void addPath(String path) {
      this.pathList.add(path);
    }

    public void addPaths(List<String> path) {
      this.pathList.addAll(path);
    }

    public void addSubQueryFromPart(SubQueryFromPart subQueryFromPart) {
      this.subQueryFromPartList.add(subQueryFromPart);
    }

    public Filter getFilter() {
      return filter;
    }

    public List<String> getPathList() {
      return pathList;
    }

    public List<SubQueryFromPart> getSubQueryFromPartList() {
      return subQueryFromPartList;
    }
  }
}
