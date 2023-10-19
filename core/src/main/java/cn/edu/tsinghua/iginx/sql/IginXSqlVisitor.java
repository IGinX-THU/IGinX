package cn.edu.tsinghua.iginx.sql;

import static cn.edu.tsinghua.iginx.engine.shared.function.FunctionUtils.isCanUseSetQuantifierFunction;
import static cn.edu.tsinghua.iginx.engine.shared.operator.MarkJoin.MARK_PREFIX;
import static cn.edu.tsinghua.iginx.sql.statement.selectstatement.SelectStatement.markJoinCount;

import cn.edu.tsinghua.iginx.engine.logical.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType;
import cn.edu.tsinghua.iginx.engine.shared.file.CSVFile;
import cn.edu.tsinghua.iginx.engine.shared.file.read.ImportCsv;
import cn.edu.tsinghua.iginx.engine.shared.file.read.ImportFile;
import cn.edu.tsinghua.iginx.engine.shared.file.write.ExportByteStream;
import cn.edu.tsinghua.iginx.engine.shared.file.write.ExportCsv;
import cn.edu.tsinghua.iginx.engine.shared.file.write.ExportFile;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.BoolFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.NotFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.OrFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.PathFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.ValueFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.AndTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.BasePreciseTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.BaseTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.OrTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.PreciseTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.WithoutTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.FuncType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.exceptions.SQLParserException;
import cn.edu.tsinghua.iginx.sql.SqlParser.AddStorageEngineStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.AggLenContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.AndExpressionContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.AndPreciseExpressionContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.AndTagExpressionContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.CancelJobStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ClearDataStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.CommitTransformJobStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.CompactStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ConstantContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.CountPointsStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.CsvFileContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.DateExpressionContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.DateFormatContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.DeleteColumnsStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.DeleteStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.DownsampleClauseContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.DropTaskStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ExportFileClauseContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ExpressionContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.FromClauseContext;
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
import cn.edu.tsinghua.iginx.sql.SqlParser.OrExpressionContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.OrPreciseExpressionContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.OrTagExpressionContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.OrderByClauseContext;
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
import cn.edu.tsinghua.iginx.sql.SqlParser.ShowClusterInfoStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ShowColumnsStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ShowConfigStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ShowEligibleJobStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ShowJobStatusStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ShowRegisterTaskStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.ShowReplicationStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.SpecialClauseContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.SqlStatementContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.StorageEngineContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.StringLiteralContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.TagEquationContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.TagExpressionContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.TagListContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.TimeIntervalContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.TimeValueContext;
import cn.edu.tsinghua.iginx.sql.SqlParser.WithClauseContext;
import cn.edu.tsinghua.iginx.sql.expression.BaseExpression;
import cn.edu.tsinghua.iginx.sql.expression.BinaryExpression;
import cn.edu.tsinghua.iginx.sql.expression.BracketExpression;
import cn.edu.tsinghua.iginx.sql.expression.ConstantExpression;
import cn.edu.tsinghua.iginx.sql.expression.Expression;
import cn.edu.tsinghua.iginx.sql.expression.Expression.ExpressionType;
import cn.edu.tsinghua.iginx.sql.expression.FromValueExpression;
import cn.edu.tsinghua.iginx.sql.expression.FuncExpression;
import cn.edu.tsinghua.iginx.sql.expression.Operator;
import cn.edu.tsinghua.iginx.sql.expression.UnaryExpression;
import cn.edu.tsinghua.iginx.sql.statement.AddStorageEngineStatement;
import cn.edu.tsinghua.iginx.sql.statement.CancelJobStatement;
import cn.edu.tsinghua.iginx.sql.statement.ClearDataStatement;
import cn.edu.tsinghua.iginx.sql.statement.CommitTransformJobStatement;
import cn.edu.tsinghua.iginx.sql.statement.CompactStatement;
import cn.edu.tsinghua.iginx.sql.statement.CountPointsStatement;
import cn.edu.tsinghua.iginx.sql.statement.DeleteColumnsStatement;
import cn.edu.tsinghua.iginx.sql.statement.DeleteStatement;
import cn.edu.tsinghua.iginx.sql.statement.DropTaskStatement;
import cn.edu.tsinghua.iginx.sql.statement.ExportFileFromSelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.InsertFromCsvStatement;
import cn.edu.tsinghua.iginx.sql.statement.InsertFromSelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.sql.statement.RegisterTaskStatement;
import cn.edu.tsinghua.iginx.sql.statement.RemoveHistoryDataSourceStatement;
import cn.edu.tsinghua.iginx.sql.statement.SetConfigStatement;
import cn.edu.tsinghua.iginx.sql.statement.ShowClusterInfoStatement;
import cn.edu.tsinghua.iginx.sql.statement.ShowColumnsStatement;
import cn.edu.tsinghua.iginx.sql.statement.ShowConfigStatement;
import cn.edu.tsinghua.iginx.sql.statement.ShowEligibleJobStatement;
import cn.edu.tsinghua.iginx.sql.statement.ShowJobStatusStatement;
import cn.edu.tsinghua.iginx.sql.statement.ShowRegisterTaskStatement;
import cn.edu.tsinghua.iginx.sql.statement.ShowReplicationStatement;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import cn.edu.tsinghua.iginx.sql.statement.frompart.FromPart;
import cn.edu.tsinghua.iginx.sql.statement.frompart.FromPartType;
import cn.edu.tsinghua.iginx.sql.statement.frompart.PathFromPart;
import cn.edu.tsinghua.iginx.sql.statement.frompart.SubQueryFromPart;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinType;
import cn.edu.tsinghua.iginx.sql.statement.selectstatement.BinarySelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.selectstatement.SelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.selectstatement.UnarySelectStatement;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.JobState;
import cn.edu.tsinghua.iginx.thrift.RemovedStorageEngineInfo;
import cn.edu.tsinghua.iginx.thrift.StorageEngine;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.thrift.UDFType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.TimeUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.antlr.v4.runtime.tree.TerminalNode;

public class IginXSqlVisitor extends SqlBaseVisitor<Statement> {

  private static final Set<FuncType> supportedAggregateWithLevelFuncSet =
      new HashSet<>(Arrays.asList(FuncType.Sum, FuncType.Count, FuncType.Avg));

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
      SelectStatement selectStatement =
          parseQueryClause(ctx.insertValuesSpec().queryClause(), false);
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
    parseInsertFullPathSpec(ctx.insertFullPathSpec(), insertStatement);

    return new InsertFromCsvStatement(importFile, insertStatement);
  }

  private void parseInsertFullPathSpec(
      InsertFullPathSpecContext ctx, InsertStatement insertStatement) {
    insertStatement.setPrefixPath(ctx.path().getText());

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
    ctx.path().forEach(e -> deleteStatement.addPath(e.getText()));
    // parse time range
    if (ctx.whereClause() != null) {
      Filter filter = parseOrExpression(ctx.whereClause().orExpression(), deleteStatement);
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
    SelectStatement selectStatement = parseQueryClause(ctx.queryClause(), false);
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
      Pair<Integer, Integer> limitAndOffset = parseLimitClause(ctx.limitClause());
      selectStatement.setLimit(limitAndOffset.getK());
      selectStatement.setOffset(limitAndOffset.getV());
    }
    if (ctx.exportFileClause() != null) {
      if (ctx.EXPLAIN() != null) {
        throw new SQLParserException(
            "OUTFILE is not supported to be used in statement with EXPLAIN.");
      }
      ExportFile exportFile = parseExportFileClause(ctx.exportFileClause());
      ExportFileFromSelectStatement statement =
          new ExportFileFromSelectStatement(selectStatement, exportFile);
      return statement;
    }
    return selectStatement;
  }

  private SelectStatement parseQueryClause(QueryClauseContext ctx, boolean isSubQuery) {
    if (ctx.inBracketQuery != null) {
      SelectStatement selectStatement = parseQueryClause(ctx.inBracketQuery, isSubQuery);
      if (ctx.orderByClause() != null) {
        parseOrderByClause(ctx.orderByClause(), selectStatement);
      }
      if (ctx.limitClause() != null) {
        Pair<Integer, Integer> limitAndOffset = parseLimitClause(ctx.limitClause());
        selectStatement.setLimit(limitAndOffset.getK());
        selectStatement.setOffset(limitAndOffset.getV());
      }
      return selectStatement;
    } else if (ctx.leftQuery != null && ctx.rightQuery != null) {
      SelectStatement leftSelectStatement = parseQueryClause(ctx.leftQuery, isSubQuery);
      SelectStatement rightSelectStatement = parseQueryClause(ctx.rightQuery, isSubQuery);
      OperatorType setOperator = parseSetOperator(ctx);
      boolean isDistinct = ctx.ALL() != null ? false : true;
      return new BinarySelectStatement(
          leftSelectStatement, setOperator, rightSelectStatement, isDistinct, isSubQuery);
    } else {
      UnarySelectStatement selectStatement = new UnarySelectStatement(isSubQuery);
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
    ctx.path().forEach(e -> deleteColumnsStatement.addPath(e.getText()));

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
    ShowColumnsStatement showColumnsStatement = new ShowColumnsStatement();
    for (PathContext pathRegex : ctx.path()) {
      showColumnsStatement.setPathRegex(pathRegex.getText());
    }
    if (ctx.withClause() != null) {
      TagFilter tagFilter = parseWithClause(ctx.withClause());
      showColumnsStatement.setTagFilter(tagFilter);
    }
    if (ctx.limitClause() != null) {
      Pair<Integer, Integer> limitAndOffset = parseLimitClause(ctx.limitClause());
      showColumnsStatement.setLimit(limitAndOffset.getK());
      showColumnsStatement.setOffset(limitAndOffset.getV());
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

  private void parseFromPaths(FromClauseContext ctx, UnarySelectStatement selectStatement) {
    List<FromPart> fromParts = new ArrayList<>();
    if (ctx.tableReference().path() != null) {
      String fromPath = ctx.tableReference().path().getText();
      if (ctx.tableReference().asClause() != null) {
        fromParts.add(new PathFromPart(fromPath, ctx.tableReference().asClause().ID().getText()));
        selectStatement.setGlobalAlias(ctx.tableReference().asClause().ID().getText());
      } else {
        fromParts.add(new PathFromPart(fromPath));
        selectStatement.setGlobalAlias(fromPath);
      }
    } else {
      SelectStatement subStatement =
          parseQueryClause(ctx.tableReference().subquery().queryClause(), true);
      if (ctx.tableReference().subquery().orderByClause() != null) {
        parseOrderByClause(ctx.tableReference().subquery().orderByClause(), subStatement);
      }
      if (ctx.tableReference().subquery().limitClause() != null) {
        Pair<Integer, Integer> limitAndOffset =
            parseLimitClause(ctx.tableReference().subquery().limitClause());
        subStatement.setLimit(limitAndOffset.getK());
        subStatement.setOffset(limitAndOffset.getV());
      }
      // 计算子查询的自由变量
      subStatement.initFreeVariables();

      if (ctx.tableReference().asClause() != null) {
        String alias = ctx.tableReference().asClause().ID().getText();
        selectStatement.setGlobalAlias(alias);
        fromParts.add(new SubQueryFromPart(subStatement, alias));
      } else {
        selectStatement.setGlobalAlias(subStatement.getGlobalAlias());
        fromParts.add(new SubQueryFromPart(subStatement));
      }
    }

    if (ctx.joinPart() != null && !ctx.joinPart().isEmpty()) {
      selectStatement.setHasJoinParts(true);

      for (JoinPartContext joinPartContext : ctx.joinPart()) {
        String pathPrefix;
        String alias = "";
        if (joinPartContext.tableReference().asClause() != null) {
          alias = joinPartContext.tableReference().asClause().ID().getText();
        }
        SelectStatement subStatement = null;
        if (joinPartContext.tableReference().path() != null) {
          pathPrefix = joinPartContext.tableReference().path().getText();
        } else {
          subStatement =
              parseQueryClause(joinPartContext.tableReference().subquery().queryClause(), true);
          if (joinPartContext.tableReference().subquery().orderByClause() != null) {
            parseOrderByClause(
                joinPartContext.tableReference().subquery().orderByClause(), subStatement);
          }
          if (joinPartContext.tableReference().subquery().limitClause() != null) {
            Pair<Integer, Integer> limitAndOffset =
                parseLimitClause(joinPartContext.tableReference().subquery().limitClause());
            subStatement.setLimit(limitAndOffset.getK());
            subStatement.setOffset(limitAndOffset.getV());
          }
          // 计算子查询的自由变量
          subStatement.initFreeVariables();
          pathPrefix = subStatement.getGlobalAlias();
        }
        if (joinPartContext.join() == null) { // cross join
          if (subStatement == null) {
            fromParts.add(new PathFromPart(pathPrefix, new JoinCondition(), alias));
          } else {
            fromParts.add(new SubQueryFromPart(subStatement, new JoinCondition(), alias));
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
          joinPartContext
              .colList()
              .path()
              .forEach(pathContext -> columns.add(pathContext.getText()));
        }

        if (subStatement == null) {
          fromParts.add(
              new PathFromPart(pathPrefix, new JoinCondition(joinType, filter, columns), alias));
        } else {
          fromParts.add(
              new SubQueryFromPart(
                  subStatement, new JoinCondition(joinType, filter, columns), alias));
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
    String configName = ctx.configName.getText();
    configName = configName.substring(1, configName.length() - 1);
    return new ShowConfigStatement(configName);
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

  private void parseSelectPathsWithValue2Meta(
      SelectClauseContext ctx, UnarySelectStatement selectStatement) {
    SelectStatement subStatement = parseQueryClause(ctx.queryClause(), false);
    if (ctx.orderByClause() != null) {
      parseOrderByClause(ctx.orderByClause(), subStatement);
    }
    if (ctx.limitClause() != null) {
      Pair<Integer, Integer> limitAndOffset = parseLimitClause(ctx.limitClause());
      subStatement.setLimit(limitAndOffset.getK());
      subStatement.setOffset(limitAndOffset.getV());
    }

    selectStatement.setExpression(new FromValueExpression(subStatement));
    selectStatement.setHasValueToSelectedPath(true);
  }

  private List<Expression> parseExpression(
      ExpressionContext ctx, UnarySelectStatement selectStatement) {
    if (ctx.functionName() != null) {
      return Collections.singletonList(parseFuncExpression(ctx, selectStatement));
    }
    if (ctx.path() != null && !ctx.path().isEmpty()) {
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
      SelectStatement subStatement = parseQueryClause(ctx.subquery().queryClause(), true);
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
                selectStatement.setSelectedPaths(baseExpression, false);
                ret.add(baseExpression);
              });
    } else {
      throw new SQLParserException("Illegal selected expression");
    }
    return ret;
  }

  private Expression parseFuncExpression(
      ExpressionContext ctx, UnarySelectStatement selectStatement) {
    String funcName = ctx.functionName().getText();

    boolean isDistinct = false;
    if (ctx.ALL() != null || ctx.DISTINCT() != null) {
      if (!isCanUseSetQuantifierFunction(funcName)) {
        throw new SQLParserException(
            "Function: " + funcName + " can't use ALL or DISTINCT in bracket.");
      }
      if (ctx.DISTINCT() != null) {
        isDistinct = true;
      }
    }

    List<String> params = new ArrayList<>();
    for (PathContext pathContext : ctx.path()) {
      params.add(pathContext.getText());
    }

    // 如果查询语句中FROM子句只有一个部分且FROM一个前缀，则SELECT子句中的path只用写出后缀
    if (!selectStatement.hasJoinParts()
        && selectStatement.getFromParts().get(0).getType() == FromPartType.PathFromPart) {
      String fromPath = selectStatement.getFromParts().get(0).getPrefix();

      List<String> newParams = new ArrayList<>();
      for (String param : params) {
        newParams.add(fromPath + SQLConstant.DOT + param);
      }
      params = newParams;
    }
    FuncExpression expression = new FuncExpression(funcName, params, isDistinct);
    selectStatement.setSelectedFuncsAndPaths(funcName, expression);
    return expression;
  }

  private Expression parseBaseExpression(
      ExpressionContext ctx, UnarySelectStatement selectStatement) {
    String selectedPath = ctx.path(0).getText();
    return parseBaseExpression(selectedPath, selectStatement);
  }

  private Expression parseBaseExpression(
      String selectedPath, UnarySelectStatement selectStatement) {
    // 如果查询语句中FROM子句只有一个部分且FROM一个前缀，则SELECT子句中的path只用写出后缀
    if (!selectStatement.hasJoinParts()
        && selectStatement.getFromParts().get(0).getType() == FromPartType.PathFromPart) {
      PathFromPart pathFromPart = (PathFromPart) selectStatement.getFromParts().get(0);
      String fullPath = pathFromPart.getPrefix() + SQLConstant.DOT + selectedPath;
      String originFullPath = pathFromPart.getOriginPrefix() + SQLConstant.DOT + selectedPath;
      BaseExpression expression = new BaseExpression(fullPath);
      selectStatement.addBaseExpression(expression);
      selectStatement.setPathSet(originFullPath);
      return expression;
    } else {
      BaseExpression expression = new BaseExpression(selectedPath);
      selectStatement.setSelectedPaths(expression);
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
    if (ctx.downsampleWithLevelClause() != null) {
      // downsampleWithLevelClause = downsampleClause + aggregateWithLevelClause
      parseDownsampleClause(ctx.downsampleWithLevelClause().downsampleClause(), selectStatement);
      parseAggregateWithLevelClause(
          ctx.downsampleWithLevelClause().aggregateWithLevelClause().INT(), selectStatement);
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
  }

  private void parseDownsampleClause(
      DownsampleClauseContext ctx, UnarySelectStatement selectStatement) {
    long precision = parseAggLen(ctx.aggLen(0));
    Pair<Long, Long> timeInterval = parseTimeInterval(ctx.timeInterval());
    selectStatement.setStartKey(timeInterval.k);
    selectStatement.setEndKey(timeInterval.v);
    selectStatement.setPrecision(precision);
    selectStatement.setSlideDistance(precision);
    selectStatement.setHasDownsample(true);
    if (ctx.STEP() != null) {
      long distance = parseAggLen(ctx.aggLen(1));
      selectStatement.setSlideDistance(distance);
    }

    // merge value filter and group time range filter
    KeyFilter startKey = new KeyFilter(Op.GE, timeInterval.k);
    KeyFilter endKey = new KeyFilter(Op.L, timeInterval.v);
    Filter mergedFilter;
    if (selectStatement.hasValueFilter()) {
      mergedFilter =
          new AndFilter(
              new ArrayList<>(Arrays.asList(selectStatement.getFilter(), startKey, endKey)));
    } else {
      mergedFilter = new AndFilter(new ArrayList<>(Arrays.asList(startKey, endKey)));
      selectStatement.setHasValueFilter(true);
    }
    selectStatement.setFilter(mergedFilter);
  }

  private void parseAggregateWithLevelClause(
      List<TerminalNode> layers, UnarySelectStatement selectStatement) {
    if (!isSupportAggregateWithLevel(selectStatement)) {
      throw new SQLParserException(
          "Aggregate with level only support aggregate query count, sum, avg for now.");
    }
    layers.forEach(
        terminalNode -> selectStatement.setLayer(Integer.parseInt(terminalNode.getText())));
  }

  private void parseGroupByClause(GroupByClauseContext ctx, UnarySelectStatement selectStatement) {
    selectStatement.setHasGroupBy(true);

    ctx.path()
        .forEach(
            pathContext -> {
              String path, originPath;
              // 如果查询语句的FROM子句只有一个部分且FROM一个前缀，则GROUP BY后的path只用写出后缀
              if (!selectStatement.hasJoinParts()
                  && selectStatement.getFromParts().get(0).getType() == FromPartType.PathFromPart) {
                PathFromPart pathFromPart = (PathFromPart) selectStatement.getFromParts().get(0);
                path = pathFromPart.getPrefix() + SQLConstant.DOT + pathContext.getText();
                originPath =
                    pathFromPart.getOriginPrefix() + SQLConstant.DOT + pathContext.getText();
              } else {
                path = pathContext.getText();
                originPath = pathContext.getText();
              }
              if (path.contains("*")) {
                throw new SQLParserException(
                    String.format("GROUP BY path '%s' has '*', which is not supported.", path));
              }
              selectStatement.setGroupByPath(path);
              selectStatement.setPathSet(originPath);
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

  private boolean isSupportAggregateWithLevel(UnarySelectStatement selectStatement) {
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
        String suffix = pathContext.getText();
        String orderByPath = suffix;
        if (selectStatement.getSelectType() == SelectStatement.SelectStatementType.UNARY) {
          UnarySelectStatement unarySelectStatement = (UnarySelectStatement) selectStatement;
          String prefix = unarySelectStatement.getFromParts().get(0).getPrefix();

          // 如果查询语句的FROM子句只有一个部分且FROM一个前缀，则ORDER BY后的path只用写出后缀
          if (!unarySelectStatement.hasJoinParts()
              && unarySelectStatement.getFromParts().get(0).getType()
                  == FromPartType.PathFromPart) {
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

  private Filter parseOrExpression(OrExpressionContext ctx, Statement statement) {
    return parseOrExpression(ctx, statement, false);
  }

  private Filter parseOrExpression(
      OrExpressionContext ctx, Statement statement, boolean isHavingFilter) {
    List<Filter> children = new ArrayList<>();
    for (AndExpressionContext andCtx : ctx.andExpression()) {
      children.add(parseAndExpression(andCtx, statement, isHavingFilter));
    }
    return children.size() == 1 ? children.get(0) : new OrFilter(children);
  }

  private Filter parseAndExpression(
      AndExpressionContext ctx, Statement statement, boolean isHavingFilter) {
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
              String.format(
                  "%s clause can not use value or path filter.", type.toString().toLowerCase()));
        }

        if (ctx.predicateWithSubquery() != null) {
          return parseFilterWithSubQuery(
              ctx.predicateWithSubquery(), (UnarySelectStatement) statement, isHavingFilter);
        } else if (ctx.path().size() == 1) {
          return parseValueFilter(ctx, (UnarySelectStatement) statement);
        } else {
          return parsePathFilter(ctx, (UnarySelectStatement) statement);
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

  private Filter parseValueFilter(PredicateContext ctx, UnarySelectStatement statement) {
    String path = ctx.path().get(0).getText();
    String originPath = path;
    // 如果查询语句不是一个子查询，FROM子句只有一个部分且FROM一个前缀，则WHERE条件中的path只用写出后缀
    if (!statement.hasJoinParts()
        && !statement.isSubQuery()
        && statement.getFromParts().get(0).getType() == FromPartType.PathFromPart) {
      PathFromPart pathFromPart = (PathFromPart) statement.getFromParts().get(0);
      path = pathFromPart.getPrefix() + SQLConstant.DOT + path;
      originPath = pathFromPart.getOriginPrefix() + SQLConstant.DOT + originPath;
    }
    if (!statement.isFreeVariable(path)) {
      statement.setPathSet(originPath);
    }

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

  private Filter parsePathFilter(PredicateContext ctx, UnarySelectStatement statement) {
    String pathA = ctx.path().get(0).getText();
    String originPathA = pathA;
    String pathB = ctx.path().get(1).getText();
    String originPathB = pathB;

    Op op = Op.str2Op(ctx.comparisonOperator().getText().trim().toLowerCase());

    // 如果查询语句不是一个子查询，FROM子句只有一个部分且FROM一个前缀，则WHERE条件中的path只用写出后缀
    if (!statement.hasJoinParts()
        && !statement.isSubQuery()
        && statement.getFromParts().get(0).getType() == FromPartType.PathFromPart) {
      PathFromPart pathFromPart = (PathFromPart) statement.getFromParts().get(0);
      originPathA = pathFromPart.getOriginPrefix() + SQLConstant.DOT + pathA;
      pathA = pathFromPart.getPrefix() + SQLConstant.DOT + pathA;
      originPathB = pathFromPart.getOriginPrefix() + SQLConstant.DOT + pathB;
      pathB = pathFromPart.getPrefix() + SQLConstant.DOT + pathB;
    }
    if (!statement.isFreeVariable(pathA)) {
      statement.setPathSet(originPathA);
    }
    if (!statement.isFreeVariable(pathB)) {
      statement.setPathSet(originPathB);
    }
    return new PathFilter(pathA, op, pathB);
  }

  private Filter parseFilterWithSubQuery(
      PredicateWithSubqueryContext ctx, UnarySelectStatement statement, boolean isHavingFilter) {
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

  private Filter parseExistsFilter(
      PredicateWithSubqueryContext ctx, UnarySelectStatement statement, boolean isHavingFilter) {
    SelectStatement subStatement = parseQueryClause(ctx.subquery().get(0).queryClause(), true);

    if (ctx.subquery().get(0).orderByClause() != null
        && ctx.subquery().get(0).limitClause() != null) {
      parseOrderByClause(ctx.subquery().get(0).orderByClause(), subStatement);
      Pair<Integer, Integer> limitAndOffset = parseLimitClause(ctx.subquery().get(0).limitClause());
      subStatement.setLimit(limitAndOffset.getK());
      subStatement.setOffset(limitAndOffset.getV());
    } else if (ctx.subquery().get(0).orderByClause() != null
        ^ ctx.subquery().get(0).limitClause() != null) {
      throw new SQLParserException(
          "order by and limit should be used at the same time in where subquery");
    }

    // 计算子查询的自由变量
    subStatement.initFreeVariables();
    String markColumn = MARK_PREFIX + markJoinCount;
    markJoinCount += 1;

    Filter filter = new BoolFilter(true);

    boolean isAntiJoin = ctx.OPERATOR_NOT() != null;
    SubQueryFromPart subQueryPart =
        new SubQueryFromPart(
            subStatement, new JoinCondition(JoinType.MarkJoin, filter, markColumn, isAntiJoin));
    if (isHavingFilter) {
      statement.addHavingSubQueryPart(subQueryPart);
    } else {
      statement.addWhereSubQueryPart(subQueryPart);
    }
    return new ValueFilter(markColumn, Op.E, new Value(true));
  }

  private Filter parseInFilter(
      PredicateWithSubqueryContext ctx, UnarySelectStatement statement, boolean isHavingFilter) {
    SelectStatement subStatement = parseQueryClause(ctx.subquery().get(0).queryClause(), true);

    if (ctx.subquery().get(0).orderByClause() != null
        && ctx.subquery().get(0).limitClause() != null) {
      parseOrderByClause(ctx.subquery().get(0).orderByClause(), subStatement);
      Pair<Integer, Integer> limitAndOffset = parseLimitClause(ctx.subquery().get(0).limitClause());
      subStatement.setLimit(limitAndOffset.getK());
      subStatement.setOffset(limitAndOffset.getV());
    } else if (ctx.subquery().get(0).orderByClause() != null
        ^ ctx.subquery().get(0).limitClause() != null) {
      throw new SQLParserException(
          "order by and limit should be used at the same time in where subquery");
    }
    if (subStatement.getExpressions().size() != 1) {
      throw new SQLParserException(
          "The number of columns in sub-query doesn't equal to outer row.");
    }
    // 计算子查询的自由变量
    subStatement.initFreeVariables();
    String markColumn = MARK_PREFIX + markJoinCount;
    markJoinCount += 1;

    Filter filter;
    if (ctx.constant() != null) {
      Value value = new Value(parseValue(ctx.constant()));
      String path = subStatement.getExpressions().get(0).getColumnName();
      filter = new ValueFilter(path, Op.E, value);
    } else {
      String pathA = ctx.path().getText();
      if (!statement.hasJoinParts()
          && !statement.isSubQuery()
          && statement.getFromParts().get(0).getType() == FromPartType.PathFromPart) {
        pathA = statement.getFromParts().get(0).getPrefix() + SQLConstant.DOT + pathA;
      }
      // deal with having filter with functions
      if (ctx.functionName() != null) {
        pathA = ctx.functionName().getText() + "(" + pathA + ")";
      }

      String pathB = subStatement.getExpressions().get(0).getColumnName();
      filter = new PathFilter(pathA, Op.E, pathB);
      subStatement.addFreeVariable(pathA);
    }

    boolean isAntiJoin = ctx.OPERATOR_NOT() != null;
    SubQueryFromPart subQueryPart =
        new SubQueryFromPart(
            subStatement, new JoinCondition(JoinType.MarkJoin, filter, markColumn, isAntiJoin));
    if (isHavingFilter) {
      statement.addHavingSubQueryPart(subQueryPart);
    } else {
      statement.addWhereSubQueryPart(subQueryPart);
    }
    return new ValueFilter(markColumn, Op.E, new Value(true));
  }

  private Filter parseQuantifierComparisonFilter(
      PredicateWithSubqueryContext ctx, UnarySelectStatement statement, boolean isHavingFilter) {
    SelectStatement subStatement = parseQueryClause(ctx.subquery().get(0).queryClause(), true);

    if (ctx.subquery().get(0).orderByClause() != null
        && ctx.subquery().get(0).limitClause() != null) {
      parseOrderByClause(ctx.subquery().get(0).orderByClause(), subStatement);
      Pair<Integer, Integer> limitAndOffset = parseLimitClause(ctx.subquery().get(0).limitClause());
      subStatement.setLimit(limitAndOffset.getK());
      subStatement.setOffset(limitAndOffset.getV());
    } else if (ctx.subquery().get(0).orderByClause() != null
        ^ ctx.subquery().get(0).limitClause() != null) {
      throw new SQLParserException(
          "order by and limit should be used at the same time in where subquery");
    }
    if (subStatement.getExpressions().size() != 1) {
      throw new SQLParserException(
          "The number of columns in sub-query doesn't equal to outer row.");
    }
    // 计算子查询的自由变量
    subStatement.initFreeVariables();
    String markColumn = MARK_PREFIX + markJoinCount;
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
      if (!statement.hasJoinParts()
          && !statement.isSubQuery()
          && statement.getFromParts().get(0).getType() == FromPartType.PathFromPart) {
        pathA = statement.getFromParts().get(0).getPrefix() + SQLConstant.DOT + pathA;
      }
      // deal with having filter with functions
      if (ctx.functionName() != null) {
        pathA = ctx.functionName().getText() + "(" + pathA + ")";
      }

      String pathB = subStatement.getExpressions().get(0).getColumnName();
      filter = new PathFilter(pathA, op, pathB);
      subStatement.addFreeVariable(pathA);
    }

    boolean isAntiJoin = ctx.quantifier().all() != null;
    SubQueryFromPart subQueryPart =
        new SubQueryFromPart(
            subStatement, new JoinCondition(JoinType.MarkJoin, filter, markColumn, isAntiJoin));
    if (isHavingFilter) {
      statement.addHavingSubQueryPart(subQueryPart);
    } else {
      statement.addWhereSubQueryPart(subQueryPart);
    }
    return new ValueFilter(markColumn, Op.E, new Value(true));
  }

  private Filter parseScalarSubQueryComparisonFilter(
      PredicateWithSubqueryContext ctx, UnarySelectStatement statement, boolean isHavingFilter) {
    SelectStatement subStatement = parseQueryClause(ctx.subquery().get(0).queryClause(), true);

    if (ctx.subquery().get(0).orderByClause() != null
        && ctx.subquery().get(0).limitClause() != null) {
      parseOrderByClause(ctx.subquery().get(0).orderByClause(), subStatement);
      Pair<Integer, Integer> limitAndOffset = parseLimitClause(ctx.subquery().get(0).limitClause());
      subStatement.setLimit(limitAndOffset.getK());
      subStatement.setOffset(limitAndOffset.getV());
    } else if (ctx.subquery().get(0).orderByClause() != null
        ^ ctx.subquery().get(0).limitClause() != null) {
      throw new SQLParserException(
          "order by and limit should be used at the same time in where subquery");
    }
    if (subStatement.getExpressions().size() != 1) {
      throw new SQLParserException(
          "The number of columns in sub-query doesn't equal to outer row.");
    }
    // 计算子查询的自由变量
    subStatement.initFreeVariables();

    Filter filter = new BoolFilter(true);

    SubQueryFromPart subQueryPart =
        new SubQueryFromPart(subStatement, new JoinCondition(JoinType.SingleJoin, filter));
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
      if (!statement.hasJoinParts()
          && !statement.isSubQuery()
          && statement.getFromParts().get(0).getType() == FromPartType.PathFromPart) {
        pathA = statement.getFromParts().get(0).getPrefix() + SQLConstant.DOT + pathA;
      }
      // deal with having filter with functions
      if (ctx.functionName() != null) {
        pathA = ctx.functionName().getText() + "(" + pathA + ")";
      }

      String pathB = subStatement.getExpressions().get(0).getColumnName();
      return new PathFilter(pathA, op, pathB);
    }
  }

  private Filter parseTwoScalarSubQueryComparisonFilter(
      PredicateWithSubqueryContext ctx, UnarySelectStatement statement, boolean isHavingFilter) {
    List<String> paths = new ArrayList<>();

    for (int i = 0; i < 2; i++) {
      SelectStatement subStatement = parseQueryClause(ctx.subquery().get(i).queryClause(), true);

      if (ctx.subquery().get(0).orderByClause() != null
          && ctx.subquery().get(0).limitClause() != null) {
        parseOrderByClause(ctx.subquery().get(0).orderByClause(), subStatement);
        Pair<Integer, Integer> limitAndOffset =
            parseLimitClause(ctx.subquery().get(0).limitClause());
        subStatement.setLimit(limitAndOffset.getK());
        subStatement.setOffset(limitAndOffset.getV());
      } else if (ctx.subquery().get(0).orderByClause() != null
          ^ ctx.subquery().get(0).limitClause() != null) {
        throw new SQLParserException(
            "order by and limit should be used at the same time in where subquery");
      }
      if (subStatement.getExpressions().size() != 1) {
        throw new SQLParserException(
            "The number of columns in sub-query doesn't equal to outer row.");
      }
      // 计算子查询的自由变量
      subStatement.initFreeVariables();
      paths.add(subStatement.getExpressions().get(0).getColumnName());

      Filter filter = new BoolFilter(true);

      SubQueryFromPart subQueryPart =
          new SubQueryFromPart(subStatement, new JoinCondition(JoinType.SingleJoin, filter));
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
    extra =
        extra.substring(extra.indexOf(SQLConstant.QUOTE) + 1, extra.lastIndexOf(SQLConstant.QUOTE));
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
}
