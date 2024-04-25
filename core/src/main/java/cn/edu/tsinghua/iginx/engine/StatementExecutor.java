package cn.edu.tsinghua.iginx.engine;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.CLEAR_DUMMY_DATA_CAUTION;
import static cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils.moveForwardNotNull;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.distributedquery.coordinator.*;
import cn.edu.tsinghua.iginx.engine.logical.constraint.ConstraintChecker;
import cn.edu.tsinghua.iginx.engine.logical.constraint.ConstraintCheckerManager;
import cn.edu.tsinghua.iginx.engine.logical.generator.DeleteGenerator;
import cn.edu.tsinghua.iginx.engine.logical.generator.InsertGenerator;
import cn.edu.tsinghua.iginx.engine.logical.generator.LogicalGenerator;
import cn.edu.tsinghua.iginx.engine.logical.generator.QueryGenerator;
import cn.edu.tsinghua.iginx.engine.logical.generator.ShowColumnsGenerator;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngine;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngineImpl;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream.EmptyRowStream;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.visitor.TaskInfoVisitor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.engine.shared.ResultUtils;
import cn.edu.tsinghua.iginx.engine.shared.constraint.ConstraintManager;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.file.FileType;
import cn.edu.tsinghua.iginx.engine.shared.file.read.ImportCsv;
import cn.edu.tsinghua.iginx.engine.shared.file.read.ImportFile;
import cn.edu.tsinghua.iginx.engine.shared.file.write.ExportByteStream;
import cn.edu.tsinghua.iginx.engine.shared.file.write.ExportCsv;
import cn.edu.tsinghua.iginx.engine.shared.file.write.ExportFile;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.visitor.OperatorInfoVisitor;
import cn.edu.tsinghua.iginx.engine.shared.processor.PostExecuteProcessor;
import cn.edu.tsinghua.iginx.engine.shared.processor.PostLogicalProcessor;
import cn.edu.tsinghua.iginx.engine.shared.processor.PostParseProcessor;
import cn.edu.tsinghua.iginx.engine.shared.processor.PostPhysicalProcessor;
import cn.edu.tsinghua.iginx.engine.shared.processor.PreExecuteProcessor;
import cn.edu.tsinghua.iginx.engine.shared.processor.PreLogicalProcessor;
import cn.edu.tsinghua.iginx.engine.shared.processor.PreParseProcessor;
import cn.edu.tsinghua.iginx.engine.shared.processor.PrePhysicalProcessor;
import cn.edu.tsinghua.iginx.engine.shared.processor.Processor;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SQLParserException;
import cn.edu.tsinghua.iginx.exceptions.StatusCode;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.resource.ResourceManager;
import cn.edu.tsinghua.iginx.sql.statement.DataStatement;
import cn.edu.tsinghua.iginx.sql.statement.DeleteColumnsStatement;
import cn.edu.tsinghua.iginx.sql.statement.DeleteStatement;
import cn.edu.tsinghua.iginx.sql.statement.ExportFileFromSelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.InsertFromCsvStatement;
import cn.edu.tsinghua.iginx.sql.statement.InsertFromSelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import cn.edu.tsinghua.iginx.sql.statement.SystemStatement;
import cn.edu.tsinghua.iginx.sql.statement.selectstatement.SelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.selectstatement.UnarySelectStatement;
import cn.edu.tsinghua.iginx.statistics.IStatisticsCollector;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.DataTypeInferenceUtils;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatementExecutor {

  private static final Logger logger = LoggerFactory.getLogger(StatementExecutor.class);

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final StatementBuilder builder = StatementBuilder.getInstance();

  private static final PhysicalEngine engine = PhysicalEngineImpl.getInstance();

  private static final PlanExecutor planExecutor = PlanExecutor.getInstance();

  private static final Evaluator evaluator = NaiveEvaluator.getInstance();

  private static final Splitter splitter = NaiveSplitter.getInstance();

  private static final ConstraintChecker checker =
      ConstraintCheckerManager.getInstance().getChecker(config.getConstraintChecker());
  private static final ConstraintManager constraintManager = engine.getConstraintManager();

  private static final ResourceManager resourceManager = ResourceManager.getInstance();

  private static final IMetaManager metaManager = DefaultMetaManager.getInstance();

  private static final Map<StatementType, List<LogicalGenerator>> generatorMap = new HashMap<>();

  private static final List<LogicalGenerator> queryGeneratorList = new ArrayList<>();
  private static final List<LogicalGenerator> deleteGeneratorList = new ArrayList<>();
  private static final List<LogicalGenerator> insertGeneratorList = new ArrayList<>();
  private static final List<LogicalGenerator> showTSGeneratorList = new ArrayList<>();

  private final List<PreParseProcessor> preParseProcessors = new ArrayList<>();
  private final List<PostParseProcessor> postParseProcessors = new ArrayList<>();
  private final List<PreLogicalProcessor> preLogicalProcessors = new ArrayList<>();
  private final List<PostLogicalProcessor> postLogicalProcessors = new ArrayList<>();
  private final List<PrePhysicalProcessor> prePhysicalProcessors = new ArrayList<>();
  private final List<PostPhysicalProcessor> postPhysicalProcessors = new ArrayList<>();
  private final List<PreExecuteProcessor> preExecuteProcessors = new ArrayList<>();
  private final List<PostExecuteProcessor> postExecuteProcessors = new ArrayList<>();

  private static class StatementExecutorHolder {

    private static final StatementExecutor instance = new StatementExecutor();
  }

  static {
    generatorMap.put(StatementType.SELECT, queryGeneratorList);
    generatorMap.put(StatementType.DELETE, deleteGeneratorList);
    generatorMap.put(StatementType.INSERT, insertGeneratorList);
    generatorMap.put(StatementType.SHOW_COLUMNS, showTSGeneratorList);
  }

  private StatementExecutor() {
    registerGenerator(QueryGenerator.getInstance());
    registerGenerator(DeleteGenerator.getInstance());
    registerGenerator(InsertGenerator.getInstance());
    registerGenerator(ShowColumnsGenerator.getInstance());

    try {
      String statisticsCollectorClassName =
          ConfigDescriptor.getInstance().getConfig().getStatisticsCollectorClassName();
      if (statisticsCollectorClassName != null && !statisticsCollectorClassName.equals("")) {
        Class<? extends IStatisticsCollector> statisticsCollectorClass =
            StatementExecutor.class
                .getClassLoader()
                .loadClass(statisticsCollectorClassName)
                .asSubclass(IStatisticsCollector.class);
        IStatisticsCollector statisticsCollector =
            statisticsCollectorClass.getConstructor().newInstance();
        registerPreParseProcessor(statisticsCollector.getPreParseProcessor());
        registerPostParseProcessor(statisticsCollector.getPostParseProcessor());
        registerPreLogicalProcessor(statisticsCollector.getPreLogicalProcessor());
        registerPostLogicalProcessor(statisticsCollector.getPostLogicalProcessor());
        registerPrePhysicalProcessor(statisticsCollector.getPrePhysicalProcessor());
        registerPostPhysicalProcessor(statisticsCollector.getPostPhysicalProcessor());
        registerPreExecuteProcessor(statisticsCollector.getPreExecuteProcessor());
        registerPostExecuteProcessor(statisticsCollector.getPostExecuteProcessor());
        statisticsCollector.startBroadcasting();
      }
    } catch (ClassNotFoundException
        | InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      logger.error("initial statistics collector error: ", e);
    }
  }

  public static StatementExecutor getInstance() {
    return StatementExecutorHolder.instance;
  }

  public void registerGenerator(LogicalGenerator generator) {
    if (generator != null) {
      switch (generator.getType()) {
        case Query:
          queryGeneratorList.add(generator);
          break;
        case Delete:
          deleteGeneratorList.add(generator);
          break;
        case Insert:
          insertGeneratorList.add(generator);
          break;
        case ShowColumns:
          showTSGeneratorList.add(generator);
          break;
        default:
          throw new IllegalArgumentException("unknown generator type");
      }
    }
  }

  public void registerPreParseProcessor(PreParseProcessor processor) {
    if (processor != null) {
      preParseProcessors.add(processor);
    }
  }

  public void registerPostParseProcessor(PostParseProcessor processor) {
    if (processor != null) {
      postParseProcessors.add(processor);
    }
  }

  public void registerPreLogicalProcessor(PreLogicalProcessor processor) {
    if (processor != null) {
      preLogicalProcessors.add(processor);
    }
  }

  public void registerPostLogicalProcessor(PostLogicalProcessor processor) {
    if (processor != null) {
      postLogicalProcessors.add(processor);
    }
  }

  public void registerPrePhysicalProcessor(PrePhysicalProcessor processor) {
    if (processor != null) {
      prePhysicalProcessors.add(processor);
    }
  }

  public void registerPostPhysicalProcessor(PostPhysicalProcessor processor) {
    if (processor != null) {
      postPhysicalProcessors.add(processor);
    }
  }

  public void registerPreExecuteProcessor(PreExecuteProcessor processor) {
    if (processor != null) {
      preExecuteProcessors.add(processor);
    }
  }

  public void registerPostExecuteProcessor(PostExecuteProcessor processor) {
    if (processor != null) {
      postExecuteProcessors.add(processor);
    }
  }

  public void execute(RequestContext ctx) {
    if (config.isEnableMemoryControl() && resourceManager.reject(ctx)) {
      ctx.setResult(new Result(RpcUtils.SERVICE_UNAVAILABLE));
      return;
    }
    before(ctx, preExecuteProcessors);
    if (ctx.isFromSQL()) {
      executeSQL(ctx);
    } else {
      executeStatement(ctx);
    }
    after(ctx, postExecuteProcessors);
  }

  public void executeSQL(RequestContext ctx) {
    try {
      before(ctx, preParseProcessors);
      builder.buildFromSQL(ctx);
      after(ctx, postParseProcessors);
      executeStatement(ctx);
    } catch (SQLParserException | ParseCancellationException e) {
      StatusCode statusCode = StatusCode.STATEMENT_PARSE_ERROR;
      ctx.setResult(new Result(RpcUtils.status(statusCode, e.getMessage())));
    } catch (Exception e) {
      e.printStackTrace();
      StatusCode statusCode = StatusCode.STATEMENT_EXECUTION_ERROR;
      String errMsg =
          "Execute Error: encounter error(s) when executing sql statement, "
              + "see server log for more details.";
      ctx.setResult(new Result(RpcUtils.status(statusCode, errMsg)));
    } finally {
      ctx.getResult().setSqlType(ctx.getSqlType());
    }
  }

  public void executeStatement(RequestContext ctx) {
    try {
      Statement statement = ctx.getStatement();
      if (statement instanceof DataStatement) {
        StatementType type = statement.getType();
        switch (type) {
          case SELECT:
          case DELETE:
          case INSERT:
          case SHOW_COLUMNS:
            process(ctx);
            return;
          case INSERT_FROM_SELECT:
            processInsertFromSelect(ctx);
            return;
          case COUNT_POINTS:
            processCountPoints(ctx);
            return;
          case DELETE_COLUMNS:
            processDeleteColumns(ctx);
            return;
          case CLEAR_DATA:
            processClearData(ctx);
            return;
          case INSERT_FROM_CSV:
            processInsertFromFile(ctx);
            return;
          case EXPORT_CSV_FROM_SELECT:
          case EXPORT_STREAM_FROM_SELECT:
            processExportFileFromSelect(ctx);
            return;
          default:
            throw new ExecutionException(
                String.format("Execute Error: unknown statement type [%s].", type));
        }
      } else {
        ((SystemStatement) statement).execute(ctx);
      }
    } catch (ExecutionException | PhysicalException | IOException e) {
      StatusCode statusCode = StatusCode.STATEMENT_EXECUTION_ERROR;
      ctx.setResult(new Result(RpcUtils.status(statusCode, e.getMessage())));
    } catch (Exception e) {
      logger.error(
          "unexpected exception during dispatcher memory task, please contact developer to check: ",
          e);
      StatusCode statusCode = StatusCode.SYSTEM_ERROR;
      ctx.setResult(new Result(RpcUtils.status(statusCode, e.getMessage())));
    }
  }

  private void process(RequestContext ctx) throws ExecutionException, PhysicalException {
    StatementType type = ctx.getStatement().getType();
    List<LogicalGenerator> generatorList = generatorMap.get(type);
    for (LogicalGenerator generator : generatorList) {
      before(ctx, preLogicalProcessors);
      Operator root = generator.generate(ctx);
      after(ctx, postLogicalProcessors);
      if (root == null && !metaManager.hasWritableStorageEngines()) {
        ctx.setResult(new Result(RpcUtils.SUCCESS));
        setResult(ctx, new EmptyRowStream());
        return;
      }
      if (constraintManager.check(root) && checker.check(root)) {
        if (type == StatementType.SELECT) {
          SelectStatement selectStatement = (SelectStatement) ctx.getStatement();
          if (selectStatement.isNeedLogicalExplain()) {
            if (evaluator.needDistributedQuery(root)) {
              //            if (true) {
              Plan plan = splitter.split(root);
              processExplainLogicalPlan(ctx, plan);
            } else {
              processExplainLogicalStatement(ctx, root);
            }
            return;
          }
        }

        before(ctx, prePhysicalProcessors);
        RowStream stream;
        long startTime = System.currentTimeMillis();
        if (evaluator.needDistributedQuery(root)) {
          Plan plan = splitter.split(root);
          stream = planExecutor.execute(ctx, plan);
        } else {
          stream = engine.execute(ctx, root);
        }
        long endTime = System.currentTimeMillis();
        long engineCostTime = endTime - startTime;
        logger.info("engine cost time: " + engineCostTime);
        after(ctx, postPhysicalProcessors);
        ctx.setEngineCostTime(engineCostTime);

        if (type == StatementType.SELECT) {
          SelectStatement selectStatement = (SelectStatement) ctx.getStatement();
          if (selectStatement.isNeedPhysicalExplain()) {
            processExplainPhysicalStatement(ctx);
            return;
          }
        }

        setResult(ctx, stream);
        return;
      }
    }
    throw new ExecutionException("Execute Error: can not construct a legal logical tree.");
  }

  private void processExplainLogicalPlan(RequestContext ctx, Plan plan)
      throws PhysicalException, ExecutionException {
    List<Field> fields =
        new ArrayList<>(
            Arrays.asList(
                new Field("Logical Tree", DataType.BINARY),
                new Field("Operator Type", DataType.BINARY),
                new Field("Operator Info", DataType.BINARY)));
    Header header = new Header(fields);

    List<Operator> roots = new ArrayList<>();
    roots.add(plan.getRoot());
    roots.addAll(plan.getSubPlans());

    int maxLen = 0;
    List<Object[]> cache = new ArrayList<>();
    for (int i = 0; i < roots.size(); i++) {
      Operator root = roots.get(i);
      cache.add(new Object[] {"[subplan" + i + "]: ", "".getBytes(), "".getBytes()});
      OperatorInfoVisitor visitor = new OperatorInfoVisitor();
      root.accept(visitor);
      maxLen = Math.max(maxLen, visitor.getMaxLen());
      cache.addAll(visitor.getCache());
    }
    formatTree(ctx, header, cache, maxLen);
  }

  private void processExplainLogicalStatement(RequestContext ctx, Operator root)
      throws PhysicalException, ExecutionException {
    List<Field> fields =
        new ArrayList<>(
            Arrays.asList(
                new Field("Logical Tree", DataType.BINARY),
                new Field("Operator Type", DataType.BINARY),
                new Field("Operator Info", DataType.BINARY)));
    Header header = new Header(fields);

    OperatorInfoVisitor visitor = new OperatorInfoVisitor();
    root.accept(visitor);
    formatTree(ctx, header, visitor.getCache(), visitor.getMaxLen());
  }

  private void processExplainPhysicalStatement(RequestContext ctx)
      throws PhysicalException, ExecutionException {
    PhysicalTask root = ctx.getPhysicalTree();
    List<Field> fields =
        new ArrayList<>(
            Arrays.asList(
                new Field("Physical Tree", DataType.BINARY),
                new Field("Execute Time", DataType.BINARY),
                new Field("Task Type", DataType.BINARY),
                new Field("Task Info", DataType.BINARY),
                new Field("Affect Rows", DataType.INTEGER)));
    Header header = new Header(fields);

    TaskInfoVisitor visitor = new TaskInfoVisitor();
    root.accept(visitor);
    formatTree(ctx, header, visitor.getCache(), visitor.getMaxLen());
  }

  private void formatTree(RequestContext ctx, Header header, List<Object[]> cache, int maxLen)
      throws PhysicalException, ExecutionException {
    List<Row> rows = new ArrayList<>();
    for (Object[] rowValues : cache) {
      StringBuilder str = new StringBuilder(((String) rowValues[0]));
      while (str.length() < maxLen) {
        str.append(" ");
      }
      rowValues[0] = str.toString().getBytes();
      rows.add(new Row(header, rowValues));
    }

    RowStream stream = new Table(header, rows);
    setResult(ctx, stream);
  }

  private void processExportFileFromSelect(RequestContext ctx)
      throws ExecutionException, PhysicalException, IOException {
    ExportFileFromSelectStatement statement = (ExportFileFromSelectStatement) ctx.getStatement();

    // step 1: select stage
    SelectStatement selectStatement = statement.getSelectStatement();
    RequestContext selectContext = new RequestContext(ctx.getSessionId(), selectStatement, true);
    process(selectContext);
    RowStream stream = selectContext.getResult().getResultStream();

    // step 2: export file
    ResultUtils.setResultFromRowStream(ctx, stream);
    ExportFile exportFile = statement.getExportFile();
    switch (exportFile.getType()) {
      case CSV:
        ExportCsv exportCsv = (ExportCsv) exportFile;
        ctx.getResult().setExportCsv(exportCsv);
        break;
      case BYTE_STREAM:
        ExportByteStream exportByteStream = (ExportByteStream) exportFile;
        ctx.getResult().setExportByteStreamDir(exportByteStream.getDir());
        break;
      default:
        throw new RuntimeException("Unknown export file type: " + exportFile.getType());
    }
  }

  private void processInsertFromFile(RequestContext ctx)
      throws ExecutionException, PhysicalException, IOException {
    InsertFromCsvStatement statement = (InsertFromCsvStatement) ctx.getStatement();
    ImportFile importFile = statement.getImportFile();
    InsertStatement insertStatement = statement.getSubInsertStatement();

    if (Objects.requireNonNull(importFile.getType()) == FileType.CSV) {
      ImportCsv importCsv = (ImportCsv) importFile;
      if (ctx.getLoadCSVFileByteBuffer() == null) {
        ctx.setResult(new Result(RpcUtils.SUCCESS));
        ctx.getResult().setLoadCSVPath(importCsv.getFilepath());
      } else {
        loadValuesSpecFromCsv(ctx, (ImportCsv) importFile, insertStatement);
      }

    } else {
      throw new RuntimeException("Unknown import file type: " + importFile.getType());
    }
  }

  private void loadValuesSpecFromCsv(
      RequestContext ctx, ImportCsv importCsv, InsertStatement insertStatement) throws IOException {
    final int BATCH_SIZE = config.getBatchSizeImportCsv();
    File tmpCSV = File.createTempFile("temp", ".csv");

    try (FileOutputStream fos = new FileOutputStream(tmpCSV)) {
      fos.write(ctx.getLoadCSVFileByteBuffer().array());
      fos.flush();
    } catch (IOException e) {
      throw new RuntimeException(
          "Encounter an error when writing file "
              + tmpCSV.getCanonicalPath()
              + ", because "
              + e.getMessage());
    }

    long count = 0;
    try {
      CSVParser parser =
          importCsv
              .getCSVBuilder()
              .build()
              .parse(new InputStreamReader(Files.newInputStream(tmpCSV.toPath())));

      CSVRecord tmp;
      Iterator<CSVRecord> iterator = parser.stream().iterator();
      // 跳过解析第一行
      if (importCsv.isSkippingImportHeader() && iterator.hasNext()) {
        tmp = iterator.next();
      }

      int pathSize = insertStatement.getPaths().size();
      while (iterator.hasNext()) {
        List<CSVRecord> records = new ArrayList<>(BATCH_SIZE);
        // 每次从文件中取出BATCH_SIZE行数据
        for (int n = 0; n < BATCH_SIZE && iterator.hasNext(); n++) {
          tmp = iterator.next();
          if (tmp.size() != pathSize + 1) {
            throw new RuntimeException(
                "The paths' size doesn't match csv data at line: " + tmp.getRecordNumber());
          }
          records.add(tmp);
        }

        int recordsSize = records.size();
        Long[] keys = new Long[recordsSize];
        Object[][] values = new Object[recordsSize][pathSize];
        List<DataType> types = new ArrayList<>();
        List<Bitmap> bitmaps = new ArrayList<>();

        // 填充 types
        Set<Integer> dataTypeIndex = new HashSet<>();
        for (int i = 0; i < pathSize; i++) {
          types.add(null);
        }
        for (int i = 0; i < pathSize; i++) {
          dataTypeIndex.add(i);
        }
        for (CSVRecord record : records) {
          for (int j = 0; j < pathSize; j++) {
            if (!dataTypeIndex.contains(j)) {
              continue;
            }
            DataType inferredDataType =
                DataTypeInferenceUtils.getInferredDataType(record.get(j + 1));
            if (inferredDataType != null) { // 找到每一列第一个不为 null 的值进行类型推断
              types.set(j, inferredDataType);
              dataTypeIndex.remove(j);
            }
          }
          if (dataTypeIndex.isEmpty()) {
            break;
          }
        }
        if (!dataTypeIndex.isEmpty()) {
          for (Integer index : dataTypeIndex) {
            types.set(index, DataType.BINARY);
          }
        }

        // 填充 keys, values 和 bitmaps
        for (int i = 0; i < recordsSize; i++) {
          CSVRecord record = records.get(i);
          keys[i] = Long.parseLong(record.get(0));
          Bitmap bitmap = new Bitmap(pathSize);

          int index = 0;
          for (int j = 0; j < pathSize; j++) {
            if (!record.get(j + 1).equalsIgnoreCase("null")) {
              bitmap.mark(j);
              switch (types.get(j)) {
                case BOOLEAN:
                  values[i][index] = Boolean.parseBoolean(record.get(j + 1));
                  index++;
                  break;
                case INTEGER:
                  values[i][index] = Integer.parseInt(record.get(j + 1));
                  index++;
                  break;
                case LONG:
                  values[i][index] = Long.parseLong(record.get(j + 1));
                  index++;
                  break;
                case FLOAT:
                  values[i][index] = Float.parseFloat(record.get(j + 1));
                  index++;
                  break;
                case DOUBLE:
                  values[i][index] = Double.parseDouble(record.get(j + 1));
                  index++;
                  break;
                case BINARY:
                  values[i][index] = record.get(j + 1).getBytes();
                  index++;
                  break;
                default:
              }
            }
          }
          bitmaps.add(bitmap);
        }

        insertStatement.setKeys(new ArrayList<>(Arrays.asList(keys)));
        insertStatement.setValues(values);
        insertStatement.setTypes(types);
        insertStatement.setBitmaps(bitmaps);

        RequestContext subInsertContext = new RequestContext(ctx.getSessionId(), insertStatement);
        process(subInsertContext);

        if (!subInsertContext.getResult().getStatus().equals(RpcUtils.SUCCESS)) {
          ctx.setResult(new Result(RpcUtils.FAILURE));
          return;
        }
        count += recordsSize;
      }
      ctx.setResult(new Result(RpcUtils.SUCCESS));
      ctx.getResult().setLoadCSVColumns(insertStatement.getPaths());
      ctx.getResult().setLoadCSVRecordNum(count);
    } catch (IOException e) {
      throw new RuntimeException(
          "Encounter an error when reading csv file "
              + tmpCSV.getCanonicalPath()
              + ", because "
              + e.getMessage());
    } catch (ExecutionException | PhysicalException e) {
      throw new RuntimeException(e);
    }

    Files.delete(tmpCSV.toPath());
  }

  private void processInsertFromSelect(RequestContext ctx)
      throws ExecutionException, PhysicalException {
    InsertFromSelectStatement statement = (InsertFromSelectStatement) ctx.getStatement();

    // step 1: select stage
    SelectStatement selectStatement = statement.getSubSelectStatement();
    RequestContext subSelectContext = new RequestContext(ctx.getSessionId(), selectStatement, true);
    process(subSelectContext);

    RowStream rowStream = subSelectContext.getResult().getResultStream();

    // step 2: insert stage
    InsertStatement insertStatement = statement.getSubInsertStatement();
    parseOldTagsFromHeader(rowStream.getHeader(), insertStatement);
    parseInsertValuesSpecFromRowStream(statement.getKeyOffset(), rowStream, insertStatement);
    RequestContext subInsertContext =
        new RequestContext(ctx.getSessionId(), insertStatement, ctx.isUseStream());
    process(subInsertContext);

    ctx.setResult(subInsertContext.getResult());
  }

  private void processCountPoints(RequestContext ctx) throws ExecutionException, PhysicalException {
    SelectStatement statement =
        new UnarySelectStatement(
            Collections.singletonList("*"), 0, Long.MAX_VALUE, AggregateType.COUNT);
    ctx.setStatement(statement);
    process(ctx);

    Result result = ctx.getResult();
    long pointsNum = 0;
    if (ctx.getResult().getValuesList() != null) {
      Object[] row =
          ByteUtils.getValuesByDataType(result.getValuesList().get(0), result.getDataTypes());
      pointsNum = Arrays.stream(row).mapToLong(e -> (Long) e).sum();
    }

    ctx.getResult().setPointsNum(pointsNum);
  }

  private void processDeleteColumns(RequestContext ctx)
      throws ExecutionException, PhysicalException {
    DeleteColumnsStatement deleteColumnsStatement = (DeleteColumnsStatement) ctx.getStatement();
    DeleteStatement deleteStatement =
        new DeleteStatement(
            deleteColumnsStatement.getPaths(), deleteColumnsStatement.getTagFilter());
    ctx.setStatement(deleteStatement);
    process(ctx);
  }

  private void processClearData(RequestContext ctx) throws ExecutionException, PhysicalException {
    DeleteStatement deleteStatement = new DeleteStatement(Collections.singletonList("*"));
    ctx.setStatement(deleteStatement);
    process(ctx);
  }

  private void setResult(RequestContext ctx, RowStream stream)
      throws PhysicalException, ExecutionException {
    Statement statement = ctx.getStatement();
    switch (statement.getType()) {
      case INSERT:
        ctx.setResult(new Result(RpcUtils.SUCCESS));
        break;
      case DELETE:
        DeleteStatement deleteStatement = (DeleteStatement) statement;
        if (deleteStatement.isInvolveDummyData()) {
          throw new ExecutionException(CLEAR_DUMMY_DATA_CAUTION);
        } else {
          ctx.setResult(new Result(RpcUtils.SUCCESS));
        }
        break;
      case SELECT:
        ResultUtils.setResultFromRowStream(ctx, stream);
        break;
      case SHOW_COLUMNS:
        ResultUtils.setShowTSRowStreamResult(ctx, stream);
        break;
      default:
        throw new ExecutionException(
            String.format("Execute Error: unknown statement type [%s].", statement.getType()));
    }
  }

  private void parseOldTagsFromHeader(Header header, InsertStatement insertStatement)
      throws PhysicalException, ExecutionException {
    if (insertStatement.getPaths().size() != header.getFieldSize()) {
      throw new ExecutionException("Execute Error: Insert path size and value size must be equal.");
    }
    List<Field> fields = header.getFields();
    List<Map<String, String>> tagsList = insertStatement.getTagsList();
    for (int i = 0; i < fields.size(); i++) {
      Field field = fields.get(i);
      Map<String, String> tags = tagsList.get(i);
      Map<String, String> oldTags = field.getTags();
      if (oldTags != null && !oldTags.isEmpty()) {
        if (tags == null) {
          tagsList.set(i, oldTags);
        } else {
          tags.putAll(oldTags);
        }
      }
    }
  }

  private void parseInsertValuesSpecFromRowStream(
      long offset, RowStream rowStream, InsertStatement insertStatement)
      throws PhysicalException, ExecutionException {
    Header header = rowStream.getHeader();
    if (insertStatement.getPaths().size() != header.getFieldSize()) {
      throw new ExecutionException("Execute Error: Insert path size and value size must be equal.");
    }

    List<DataType> types = new ArrayList<>();
    header.getFields().forEach(field -> types.add(field.getType()));

    List<Long> times = new ArrayList<>();
    List<Object[]> rows = new ArrayList<>();
    List<Bitmap> bitmaps = new ArrayList<>();

    for (long i = 0; rowStream.hasNext(); i++) {
      Row row = rowStream.next();
      rows.add(moveForwardNotNull(row.getValues()));

      int rowLen = row.getValues().length;
      Bitmap bitmap = new Bitmap(rowLen);
      for (int j = 0; j < rowLen; j++) {
        if (row.getValue(j) != null) {
          bitmap.mark(j);
        }
      }
      bitmaps.add(bitmap);

      if (header.hasKey()) {
        times.add(row.getKey() + offset);
      } else {
        times.add(i + offset);
      }
    }
    Object[][] values = rows.toArray(new Object[0][0]);

    insertStatement.setKeys(times);
    insertStatement.setValues(values);
    insertStatement.setTypes(types);
    insertStatement.setBitmaps(bitmaps);
  }

  private void before(RequestContext ctx, List<? extends Processor> list) {
    record(ctx, list);
  }

  private void after(RequestContext ctx, List<? extends Processor> list) {
    record(ctx, list);
  }

  private void record(RequestContext ctx, List<? extends Processor> list) {
    for (Processor processor : list) {
      Status status = processor.process(ctx);
      if (status != null) {
        ctx.setStatus(status);
        return;
      }
    }
  }
}
