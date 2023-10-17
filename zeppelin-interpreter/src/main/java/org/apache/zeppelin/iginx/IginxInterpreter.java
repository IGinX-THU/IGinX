package org.apache.zeppelin.iginx;

import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.utils.FormatUtils;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.zeppelin.interpreter.AbstractInterpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.ZeppelinContext;

public class IginxInterpreter extends AbstractInterpreter {

  private static final String IGINX_HOST = "iginx.host";
  private static final String IGINX_PORT = "iginx.port";
  private static final String IGINX_USERNAME = "iginx.username";
  private static final String IGINX_PASSWORD = "iginx.password";
  private static final String IGINX_TIME_PRECISION = "iginx.time.precision";

  private static final String DEFAULT_HOST = "127.0.0.1";
  private static final String DEFAULT_PORT = "6888";
  private static final String DEFAULT_USERNAME = "root";
  private static final String DEFAULT_PASSWORD = "root";
  private static final String DEFAULT_TIME_PRECISION = "ns";

  private static final String TAB = "\t";
  private static final String NEWLINE = "\n";
  private static final String WHITESPACE = " ";
  private static final String MULTISPACE = " +";
  private static final String SEMICOLON = ";";
  private static final String SUCCESS = "Success!";
  private static final String NO_DATA_TO_PRINT = "No data to print.\n";

  private String host = "";
  private int port = 0;
  private String username = "";
  private String password = "";
  private String timePrecision = "";
  private Session session;

  private Exception exception;

  // 返回结果为单个表格的语句
  private static final List<SqlType> singleFormSqlType =
      Arrays.asList(
          SqlType.Query,
          SqlType.ShowColumns,
          SqlType.GetReplicaNum,
          SqlType.GetReplicaNum,
          SqlType.ShowRegisterTask);

  public IginxInterpreter(Properties properties) {
    super(properties);
  }

  @Override
  public ZeppelinContext getZeppelinContext() {
    return null;
  }

  @Override
  public void open() throws InterpreterException {
    host = getProperty(IGINX_HOST, DEFAULT_HOST).trim();
    port = Integer.parseInt(getProperty(IGINX_PORT, DEFAULT_PORT).trim());
    username = properties.getProperty(IGINX_USERNAME, DEFAULT_USERNAME).trim();
    password = properties.getProperty(IGINX_PASSWORD, DEFAULT_PASSWORD).trim();
    timePrecision = properties.getProperty(IGINX_TIME_PRECISION, DEFAULT_TIME_PRECISION).trim();

    session = new Session(host, port, username, password);
    try {
      session.openSession();
    } catch (SessionException e) {
      exception = e;
      System.out.println("Can not open session successfully.");
    }
  }

  @Override
  public void close() throws InterpreterException {
    try {
      if (session != null) {
        session.closeSession();
      }
    } catch (SessionException e) {
      exception = e;
      System.out.println("Can not close session successfully.");
    }
  }

  @Override
  public InterpreterResult internalInterpret(String st, InterpreterContext context)
      throws InterpreterException {
    if (exception != null) {
      return new InterpreterResult(InterpreterResult.Code.ERROR, exception.getMessage());
    }

    String[] cmdList = parseMultiLinesSQL(st);

    InterpreterResult interpreterResult = null;
    for (String cmd : cmdList) {
      interpreterResult = processSql(cmd);
    }

    return interpreterResult;
  }

  private InterpreterResult processSql(String sql) {
    try {
      SessionExecuteSqlResult sqlResult = session.executeSql(sql);

      String parseErrorMsg = sqlResult.getParseErrorMsg();
      if (parseErrorMsg != null && !parseErrorMsg.equals("")) {
        return new InterpreterResult(InterpreterResult.Code.ERROR, sqlResult.getParseErrorMsg());
      }

      InterpreterResult interpreterResult = null;
      String msg = "";

      if (singleFormSqlType.contains(sqlResult.getSqlType()) && !sql.startsWith("explain")) {
        msg =
            buildSingleFormResult(
                sqlResult.getResultInList(true, FormatUtils.DEFAULT_TIME_FORMAT, timePrecision));
        interpreterResult = new InterpreterResult(InterpreterResult.Code.SUCCESS);
        interpreterResult.add(InterpreterResult.Type.TABLE, msg);
      } else if (sqlResult.getSqlType() == SqlType.Query && sql.startsWith("explain")) {
        msg =
            buildExplainResult(
                sqlResult.getResultInList(true, FormatUtils.DEFAULT_TIME_FORMAT, timePrecision));
        interpreterResult = new InterpreterResult(InterpreterResult.Code.SUCCESS);
        interpreterResult.add(InterpreterResult.Type.TABLE, msg);
      } else if (sqlResult.getSqlType() == SqlType.ShowClusterInfo) {
        interpreterResult = new InterpreterResult(InterpreterResult.Code.SUCCESS);
        buildClusterInfoResult(
            interpreterResult,
            sqlResult.getResultInList(true, FormatUtils.DEFAULT_TIME_FORMAT, timePrecision));
      } else {
        msg = sqlResult.getResultInString(true, timePrecision);
        if (msg.equals(NO_DATA_TO_PRINT)) {
          msg = SUCCESS;
        }
        interpreterResult = new InterpreterResult(InterpreterResult.Code.SUCCESS, msg);
      }

      return interpreterResult;
    } catch (Exception e) {
      return new InterpreterResult(
          InterpreterResult.Code.ERROR,
          "encounter error when executing sql statement:\n" + e.getMessage());
    }
  }

  private String buildSingleFormResult(List<List<String>> queryList) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < queryList.size(); i++) {
      List<String> row = queryList.get(i);
      for (String val : row) {
        if (i != 0) {
          val = convertToHTMLString(val);
        }
        builder.append(val).append(TAB);
      }
      builder.deleteCharAt(builder.length() - 1);
      builder.append(NEWLINE);
    }
    return builder.toString();
  }

  private String buildExplainResult(List<List<String>> queryList) {
    StringBuilder builder = new StringBuilder();
    for (List<String> row : queryList) {
      for (String val : row) {
        if (row.get(0).equals(val) && val.startsWith(" ")) {
          // zeppelin会将表格中的开头空格给删除，并且会将多个空格合并成一个空格，因此需要将查询树中开头的空格替换成其他字符
          for (int i = 0; i < val.length(); i++) {
            if (val.charAt(i) != ' ') {
              builder.append(val.substring(i)).append(TAB);
              break;
            } else {
              builder.append("-");
            }
          }
        } else {
          builder.append(val).append(TAB);
        }
      }
      builder.deleteCharAt(builder.length() - 1);
      builder.append(NEWLINE);
    }
    return builder.toString();
  }

  /** 构造Show Cluster Info的结果，因为返回结果为多个表格，因此需要传入InterpreterResult进行构造 */
  private void buildClusterInfoResult(
      InterpreterResult interpreterResult, List<List<String>> clusterInfoList) {
    List<String> titles =
        Arrays.asList(
            "IginX infos:", "Storage engine infos:", "Meta Storage infos:", "Meta Storage path:");
    StringBuilder builder = new StringBuilder();
    for (List<String> row : clusterInfoList) {
      if (row.size() == 1 && titles.contains(row.get(0))) {
        if (!builder.toString().isEmpty()) {
          interpreterResult.add(InterpreterResult.Type.TABLE, builder.toString());
          builder = new StringBuilder();
        }
        interpreterResult.add(InterpreterResult.Type.TEXT, row.get(0));
        continue;
      }

      for (String val : row) {
        builder.append(val).append(TAB);
      }
      builder.deleteCharAt(builder.length() - 1);
      builder.append(NEWLINE);
    }

    if (!builder.toString().isEmpty()) {
      interpreterResult.add(InterpreterResult.Type.TABLE, builder.toString());
    }
  }

  private String[] parseMultiLinesSQL(String sql) {
    String[] tmp =
        sql.replace(TAB, WHITESPACE)
            .replace(NEWLINE, WHITESPACE)
            .replaceAll(MULTISPACE, WHITESPACE)
            .trim()
            .split(SEMICOLON);
    return Arrays.stream(tmp).map(String::trim).toArray(String[]::new);
  }

  private String convertToHTMLString(String str) {
    return "%html " + str.replace("\n", "<br>").replace("\t", "&nbsp;&nbsp;&nbsp;&nbsp;");
  }

  @Override
  public void cancel(InterpreterContext context) throws InterpreterException {
    try {
      session.closeSession();
    } catch (SessionException e) {
      exception = e;
      System.out.println("Can not close session successfully.");
    }
  }

  @Override
  public FormType getFormType() throws InterpreterException {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) throws InterpreterException {
    return 0;
  }
}
