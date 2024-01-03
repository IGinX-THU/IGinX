package cn.edu.tsinghua.iginx.engine;

import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.sql.IginXSqlVisitor;
import cn.edu.tsinghua.iginx.sql.SQLParseError;
import cn.edu.tsinghua.iginx.sql.SqlLexer;
import cn.edu.tsinghua.iginx.sql.SqlParser;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import java.util.HashMap;
import java.util.Map;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

public class StatementBuilder {

  private static final Map<StatementType, SqlType> typeMap = new HashMap<>();

  static {
    typeMap.put(StatementType.INSERT, SqlType.Insert);
    typeMap.put(StatementType.DELETE, SqlType.Delete);
    typeMap.put(StatementType.SELECT, SqlType.Query);
    typeMap.put(StatementType.INSERT_FROM_SELECT, SqlType.Insert);
    typeMap.put(StatementType.INSERT_FROM_CSV, SqlType.LoadCsv);
    typeMap.put(StatementType.EXPORT_CSV_FROM_SELECT, SqlType.ExportCsv);
    typeMap.put(StatementType.EXPORT_STREAM_FROM_SELECT, SqlType.ExportStream);
    typeMap.put(StatementType.ADD_STORAGE_ENGINE, SqlType.AddStorageEngines);
    typeMap.put(StatementType.REMOVE_HISTORY_DATA_SOURCE, SqlType.RemoveHistoryDataSource);
    typeMap.put(StatementType.SHOW_REPLICATION, SqlType.GetReplicaNum);
    typeMap.put(StatementType.COUNT_POINTS, SqlType.CountPoints);
    typeMap.put(StatementType.CLEAR_DATA, SqlType.ClearData);
    typeMap.put(StatementType.DELETE_COLUMNS, SqlType.DeleteColumns);
    typeMap.put(StatementType.SHOW_COLUMNS, SqlType.ShowColumns);
    typeMap.put(StatementType.SHOW_CLUSTER_INFO, SqlType.ShowClusterInfo);
    typeMap.put(StatementType.SHOW_REGISTER_TASK, SqlType.ShowRegisterTask);
    typeMap.put(StatementType.REGISTER_TASK, SqlType.RegisterTask);
    typeMap.put(StatementType.DROP_TASK, SqlType.DropTask);
    typeMap.put(StatementType.COMMIT_TRANSFORM_JOB, SqlType.CommitTransformJob);
    typeMap.put(StatementType.SHOW_JOB_STATUS, SqlType.ShowJobStatus);
    typeMap.put(StatementType.CANCEL_JOB, SqlType.CancelJob);
    typeMap.put(StatementType.SHOW_ELIGIBLE_JOB, SqlType.ShowEligibleJob);
    typeMap.put(StatementType.COMPACT, SqlType.Compact);
    typeMap.put(StatementType.SET_CONFIG, SqlType.SetConfig);
    typeMap.put(StatementType.SHOW_CONFIG, SqlType.ShowConfig);
    typeMap.put(StatementType.SHOW_SESSION_ID, SqlType.ShowSessionID);
    typeMap.put(StatementType.SHOW_RULES, SqlType.ShowRules);
    typeMap.put(StatementType.SET_RULES, SqlType.SetRules);
  }

  private static final StatementBuilder instance = new StatementBuilder();

  private StatementBuilder() {}

  public static StatementBuilder getInstance() {
    return instance;
  }

  public void buildFromSQL(RequestContext ctx) {
    String sql = ctx.getSql();
    SqlLexer lexer = new SqlLexer(CharStreams.fromString(sql));
    lexer.removeErrorListeners();
    lexer.addErrorListener(SQLParseError.INSTANCE);

    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SqlParser parser = new SqlParser(tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(SQLParseError.INSTANCE);

    IginXSqlVisitor visitor = new IginXSqlVisitor();
    ParseTree tree = parser.sqlStatement();
    Statement statement = visitor.visit(tree);
    ctx.setStatement(statement);
    ctx.setSqlType(typeMap.get(statement.getType()));
  }
}
