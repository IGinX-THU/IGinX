/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.client;

import static cn.edu.tsinghua.iginx.utils.CSVUtils.getCSVBuilder;
import static cn.edu.tsinghua.iginx.utils.FileUtils.exportByteStream;

import cn.edu.tsinghua.iginx.constant.GlobalConstant;
import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session.QueryDataSet;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.ExportCSV;
import cn.edu.tsinghua.iginx.thrift.FileChunk;
import cn.edu.tsinghua.iginx.thrift.LoadUDFResp;
import cn.edu.tsinghua.iginx.utils.FormatUtils;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.io.*;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidParameterException;
import java.util.*;
import org.apache.commons.cli.*;
import org.apache.commons.csv.CSVPrinter;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.reader.impl.completer.NullCompleter;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

/** args[]: -h 127.0.0.1 -p 6888 -u root -pw root */
public class IginxClient {

  private static final String IGINX_CLI_PREFIX = "IGinX> ";
  private static final String IGINX_CLI_PREFIX_WAITING_INPUT = "     > ";

  private static final String HOST_ARGS = "h";
  private static final String HOST_NAME = "host";

  private static final String PORT_ARGS = "p";
  private static final String PORT_NAME = "port";

  private static final String USERNAME_ARGS = "u";
  private static final String USERNAME_NAME = "username";

  private static final String PASSWORD_ARGS = "pw";
  private static final String PASSWORD_NAME = "password";

  private static final String EXECUTE_ARGS = "e";
  private static final String EXECUTE_NAME = "execute";

  private static final String FETCH_SIZE_ARGS = "fs";
  private static final String FETCH_SIZE_NAME = "fetch_size";

  private static final String HELP_ARGS = "help";

  private static final int MAX_HELP_CONSOLE_WIDTH = 88;

  private static final String SCRIPT_HINT = "./start-cli.sh(start-cli.bat if Windows)";

  private static final String QUIT_COMMAND = "quit;";
  private static final String EXIT_COMMAND = "exit;";

  static String host = "127.0.0.1";
  static String port = "6888";
  static String username = "root";
  static String password = "root";
  static String fetchSize = "1000";

  static String execute = "";

  private static int MAX_GETDATA_NUM = 100;
  private static String timestampPrecision = "";
  private static final Set<String> legalTimeUnitSet =
      new HashSet<>(Arrays.asList("week", "day", "hour", "min", "s", "ns", "us", "ns"));

  private static CommandLine commandLine;
  private static Session session;

  private static final StringBuilder cache = new StringBuilder();

  private static Options createOptions() {
    Options options = new Options();

    options.addOption(HELP_ARGS, false, "Display help information(optional)");
    options.addOption(HOST_ARGS, HOST_NAME, true, "Host Name (optional, default 127.0.0.1)");
    options.addOption(PORT_ARGS, PORT_NAME, true, "Port (optional, default 6888)");
    options.addOption(USERNAME_ARGS, USERNAME_NAME, true, "User name (optional, default \"root\")");
    options.addOption(PASSWORD_ARGS, PASSWORD_NAME, true, "Password (optional, default \"root\")");
    options.addOption(EXECUTE_ARGS, EXECUTE_NAME, true, "Execute (optional)");
    options.addOption(
        FETCH_SIZE_ARGS, FETCH_SIZE_NAME, true, "Fetch size per query (optional, default 1000)");

    return options;
  }

  private static boolean parseCommandLine(Options options, String[] args, HelpFormatter hf) {
    try {
      CommandLineParser parser = new DefaultParser();
      commandLine = parser.parse(options, args);
      if (commandLine.hasOption(HELP_ARGS)) {
        hf.printHelp(SCRIPT_HINT, options, true);
        return false;
      }
    } catch (ParseException e) {
      System.out.println(
          "Require more params input, eg. ./start-cli.sh(start-cli.bat if Windows) "
              + "-h xxx.xxx.xxx.xxx -p xxxx -u xxx -pw xxx -fs xxx.");
      System.out.println("For more information, please check the following hint.");
      hf.printHelp(SCRIPT_HINT, options, true);
      return false;
    }
    return true;
  }

  public static void main(String[] args) {
    Options options = createOptions();

    HelpFormatter hf = new HelpFormatter();
    hf.setWidth(MAX_HELP_CONSOLE_WIDTH);

    if (args == null || args.length == 0) {
      System.out.println(
          "Require more params input, eg. ./start-cli.sh(start-cli.bat if Windows) "
              + "-h xxx.xxx.xxx.xxx -p xxxx -u xxx -p xxx -fs xxx.");
      System.out.println("For more information, please check the following hint.");
      hf.printHelp(SCRIPT_HINT, options, true);
      return;
    }

    if (!parseCommandLine(options, args, hf)) {
      return;
    }

    // make sure session is closed when client is shutdown.
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    if (!session.isClosed()) {
                      session.closeSession();
                    }
                  } catch (SessionException e) {
                    e.printStackTrace();
                  }
                }));

    serve(args);
  }

  private static String parseArg(String arg, String name, boolean isRequired, String defaultValue) {
    String str = commandLine.getOptionValue(arg);
    if (str == null) {
      if (isRequired && defaultValue == null) {
        String msg =
            String.format(
                "%s Required values for option '%s' not provided", IGINX_CLI_PREFIX, name);
        System.out.println(msg);
        System.out.println("Use -help for more information");
        throw new RuntimeException();
      }
      return defaultValue;
    }
    return str;
  }

  private static void serve(String[] args) {
    try {
      Terminal terminal = TerminalBuilder.builder().system(true).build();

      LineReader reader =
          LineReaderBuilder.builder().terminal(terminal).completer(buildIginxCompleter()).build();

      host = parseArg(HOST_ARGS, HOST_NAME, false, "127.0.0.1");
      port = parseArg(PORT_ARGS, PORT_NAME, false, "6888");
      username = parseArg(USERNAME_ARGS, USERNAME_NAME, false, "root");
      password = parseArg(PASSWORD_ARGS, PASSWORD_NAME, false, "root");
      execute = parseArg(EXECUTE_ARGS, EXECUTE_NAME, false, "");
      fetchSize = parseArg(FETCH_SIZE_ARGS, FETCH_SIZE_NAME, false, "1000");

      session = new Session(host, port, username, password);
      session.openSession();

      if (execute.equals("")) {
        echoStarting();
        displayLogo(loadClientVersion());

        String command;
        while (true) {
          if (cache.toString().trim().isEmpty()) {
            command = reader.readLine(IGINX_CLI_PREFIX);
          } else {
            command = reader.readLine(IGINX_CLI_PREFIX_WAITING_INPUT);
          }
          boolean continues = processCommand(command);
          if (!continues) {
            break;
          }
        }
        session.closeSession();
        System.out.println("Goodbye");
      } else {
        processCommand(parseExecuteCommand(args));
      }
    } catch (UserInterruptException e) {
      try {
        session.closeSession();
      } catch (SessionException ex) {
        System.out.println("Unable to close session.");
      }
      System.out.println("Goodbye");
    } catch (RuntimeException e) {
      System.out.println(IGINX_CLI_PREFIX + "Parse Parameter error: " + e.getMessage());
      System.out.println(IGINX_CLI_PREFIX + "Use -help for more information");
    } catch (Exception e) {
      System.out.println(IGINX_CLI_PREFIX + "exit cli with error " + e.getMessage());
    }
  }

  private static String loadClientVersion() {
    URL url =
        IginxClient.class.getResource(
            "/META-INF/maven/cn.edu.tsinghua/iginx-client/pom.properties");
    if (url != null) {
      Properties properties = new Properties();
      try {
        properties.load(url.openStream());
        return properties.getProperty("version");
      } catch (Exception e) {
        System.err.println("Failed to load version: " + e);
      }
    }

    return "unknown";
  }

  private static boolean processCommand(String command) throws SessionException, IOException {
    if (command == null || command.trim().isEmpty()) {
      return true;
    }
    String[] cmds = command.split(";", -1);
    int lastIndex = cmds.length - 1;
    for (int i = 0; i < lastIndex; i++) {
      cache.append(cmds[i]);
      if (cache.toString().trim().isEmpty()) {
        continue;
      }
      cache.append(";");
      OperationResult res = handleInputStatement(cache.toString());
      cache.setLength(0);
      switch (res) {
        case STOP:
          return false;
        case CONTINUE:
          continue;
        default:
          break;
      }
    }
    cache.append(cmds[lastIndex]).append(System.lineSeparator());
    return true;
  }

  private static OperationResult handleInputStatement(String statement)
      throws SessionException, IOException {
    String trimedStatement = statement.replaceAll(" +", " ").toLowerCase().trim();

    if (trimedStatement.equals(EXIT_COMMAND) || trimedStatement.equals(QUIT_COMMAND)) {
      return OperationResult.STOP;
    }
    long startTime = System.currentTimeMillis();
    if (isQuery(trimedStatement)) {
      processSqlWithStream(statement);
    } else if (isLoadDataFromCsv(trimedStatement)) {
      processLoadCsv(statement);
    } else if (isSetTimeUnit(trimedStatement)) {
      processSetTimeUnit(statement);
    } else if (isRegisterPy(trimedStatement)) {
      processPythonRegister(statement);
    } else if (isCommitTransformJob(trimedStatement)) {
      processCommitTransformJob(statement);
    } else {
      processSql(statement);
    }
    long endTime = System.currentTimeMillis();
    System.out.printf("Time cost: %d ms\n", endTime - startTime);
    return OperationResult.DO_NOTHING;
  }

  private static boolean isRegisterPy(String sql) {
    return sql.startsWith("create") && sql.contains("function");
  }

  private static boolean isQuery(String sql) {
    return sql.startsWith("select") || sql.startsWith("with");
  }

  private static boolean isLoadDataFromCsv(String sql) {
    return sql.startsWith("load data from infile ") && sql.contains("as csv");
  }

  private static boolean isSetTimeUnit(String sql) {
    return sql.startsWith("set time unit in");
  }

  private static boolean isCommitTransformJob(String sql) {
    return sql.startsWith("commit") && sql.contains("transform") && sql.contains("job");
  }

  private static void processPythonRegister(String sql) {
    try {
      String parseErrorMsg;
      LoadUDFResp resp = session.executeRegisterTask(sql);
      parseErrorMsg = resp.getParseErrorMsg();
      if (parseErrorMsg != null && !parseErrorMsg.equals("")) {
        System.out.println(resp.getParseErrorMsg());
        return;
      }

      System.out.println("success");
    } catch (SessionException e) {
      System.out.println(e.getMessage());
    } catch (Exception e) {
      System.out.println(
          "Execute Error: encounter error(s) when executing sql statement, "
              + "see server log for more details.");
    }
  }

  private static void processSetTimeUnit(String sql) {
    String[] args = sql.split(" ");
    if (args.length != 5 || !legalTimeUnitSet.contains(args[4])) {
      System.out.println(
          "Legal clause: set time unit in xx, " + "xx can be week, day, hour, min, s, ns, us, ns");
      return;
    }
    timestampPrecision = args[4];
    System.out.printf("Current time unit: %s\n", timestampPrecision);
  }

  private static void processCommitTransformJob(String sql) {
    try {
      long id = session.commitTransformJob(sql);
      System.out.println("job id: " + id);
    } catch (SessionException e) {
      System.out.println(e.getMessage());
    }
  }

  private static boolean isSetTimeUnit() {
    return !timestampPrecision.equals("");
  }

  private static void processSql(String sql) {
    try {
      SessionExecuteSqlResult res = session.executeSql(sql);

      String parseErrorMsg = res.getParseErrorMsg();
      if (parseErrorMsg != null && !parseErrorMsg.equals("")) {
        System.out.println(res.getParseErrorMsg());
        return;
      }

      switch (res.getSqlType()) {
        case Query:
          res.print(isSetTimeUnit(), timestampPrecision);
          break;
        case ShowColumns:
        case ShowClusterInfo:
        case ShowRegisterTask:
        case ShowEligibleJob:
        case ShowConfig:
        case CommitTransformJob:
        case ShowJobStatus:
        case ShowSessionID:
        case ShowRules:
        case ShowUser:
          res.print(false, "");
          break;
        case GetReplicaNum:
          System.out.println(res.getReplicaNum());
          System.out.println("success");
          break;
        case CountPoints:
          System.out.println(res.getPointsNum());
          System.out.println("success");
          break;
        default:
          System.out.println("success");
      }
    } catch (SessionException e) {
      System.out.println(e.getMessage());
    } catch (Exception e) {
      System.out.println(
          "Execute Error: encounter error(s) when executing sql statement, "
              + "see server log for more details.");
    }
  }

  private static void processSqlWithStream(String sql) {
    try {
      QueryDataSet res = session.executeQuery(sql, Integer.parseInt(fetchSize));

      if (res.getExportStreamDir() != null) {
        processExportByteStream(res);
        return;
      } else if (res.getExportCSV() != null) {
        processExportCsv(res);
        return;
      }

      if (res.getWarningMsg() != null && !res.getWarningMsg().isEmpty()) {
        System.out.println("[WARN] " + res.getWarningMsg());
      }

      System.out.println("ResultSets:");

      List<List<String>> cache = cacheResult(res);
      System.out.print(FormatUtils.formatResult(cache));

      boolean isCancelled = false;
      int total = cache.size() - 1;

      while (res.hasMore()) {
        System.out.printf(
            "Reach the max_display_num = %s. Press ENTER to show more, input 'q' to quit.",
            Integer.parseInt(fetchSize));
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        try {
          if ("".equals(br.readLine())) {
            cache = cacheResult(res);
            System.out.print(FormatUtils.formatResult(cache));
            total += cache.size() - 1;
          } else {
            isCancelled = true;
            break;
          }
        } catch (IOException e) {
          System.out.println("IO Error: " + e.getMessage());
          isCancelled = true;
          break;
        }
      }
      if (!isCancelled) {
        System.out.print(FormatUtils.formatCount(total));
      }
      res.close();
    } catch (SessionException e) {
      System.out.println(e.getMessage());
    } catch (Exception e) {
      System.out.println(
          "Execute Error: encounter error(s) when executing sql statement, "
              + "see server log for more details.");
    }
  }

  private static List<List<String>> cacheResult(QueryDataSet queryDataSet) throws SessionException {
    return cacheResult(queryDataSet, false);
  }

  private static List<List<String>> cacheResult(QueryDataSet queryDataSet, boolean skipHeader)
      throws SessionException {
    boolean hasKey = queryDataSet.getColumnList().get(0).equals(GlobalConstant.KEY_NAME);
    List<List<String>> cache = new ArrayList<>();
    if (!skipHeader) {
      cache.add(new ArrayList<>(queryDataSet.getColumnList()));
    }

    int rowIndex = 0;
    while (queryDataSet.hasMore() && rowIndex < Integer.parseInt(fetchSize)) {
      List<String> strRow = new ArrayList<>();
      Object[] nextRow = queryDataSet.nextRow();
      if (nextRow != null) {
        if (hasKey && isSetTimeUnit()) {
          strRow.add(
              FormatUtils.formatTime(
                  (Long) nextRow[0], FormatUtils.DEFAULT_TIME_FORMAT, timestampPrecision));
          for (int i = 1; i < nextRow.length; i++) {
            strRow.add(FormatUtils.valueToString(nextRow[i]));
          }
        } else {
          Arrays.stream(nextRow).forEach(val -> strRow.add(FormatUtils.valueToString(val)));
        }
        cache.add(strRow);
        rowIndex++;
      }
    }
    return cache;
  }

  private static void processExportByteStream(QueryDataSet res)
      throws SessionException, IOException {
    String dir = res.getExportStreamDir();

    File dirFile = new File(dir);
    if (!dirFile.exists()) {
      Files.createDirectory(Paths.get(dir));
    }
    if (!dirFile.isDirectory()) {
      throw new InvalidParameterException(dir + " is not a directory!");
    }

    int columnsSize = res.getColumnList().size();
    int finalCnt = columnsSize;
    String[] columns = new String[columnsSize];
    Map<String, Integer> countMap = new HashMap<>();
    for (int i = 0; i < columnsSize; i++) {
      String originColumn = res.getColumnList().get(i);
      if (originColumn.equals(GlobalConstant.KEY_NAME)) {
        columns[i] = "";
        finalCnt--;
        continue;
      }
      originColumn = originColumn.replace("\\", ".");
      Integer count = countMap.getOrDefault(originColumn, 0);
      count += 1;
      countMap.put(originColumn, count);
      // 重复的列名在列名后面加上(1),(2)...
      if (count >= 2) {
        columns[i] = Paths.get(dir, originColumn + "(" + (count - 1) + ")").toString();
      } else {
        columns[i] = Paths.get(dir, originColumn).toString();
      }
      // 若将要写入的文件存在，删除之
      Files.deleteIfExists(Paths.get(columns[i]));
    }

    while (res.hasMore()) {
      List<List<byte[]>> cache = cacheResultByteArray(res);
      exportByteStream(cache, columns);
    }
    res.close();

    System.out.println(
        "Successfully write "
            + finalCnt
            + " file(s) to directory: \""
            + dirFile.getAbsolutePath()
            + "\".");
  }

  private static List<List<byte[]>> cacheResultByteArray(QueryDataSet queryDataSet)
      throws SessionException {
    List<List<byte[]>> cache = new ArrayList<>();
    int rowIndex = 0;
    while (queryDataSet.hasMore() && rowIndex < Integer.parseInt(fetchSize)) {
      List<byte[]> nextRow = queryDataSet.nextRowAsBytes();
      if (nextRow != null) {
        cache.add(nextRow);
        rowIndex++;
      }
    }
    return cache;
  }

  private static void processExportCsv(QueryDataSet res) throws SessionException, IOException {
    ExportCSV exportCSV = res.getExportCSV();

    String path = exportCSV.getExportCsvPath();
    if (!path.endsWith(".csv")) {
      System.out.println(
          "The file name must end with [.csv], " + path + " doesn't satisfy the requirement!");
      return;
    }

    File file = new File(path);
    // 删除原来的csv文件，新建一个新的csv文件
    Files.deleteIfExists(Paths.get(file.getPath()));
    Files.createFile(Paths.get(file.getPath()));
    if (!file.isFile()) {
      System.out.println(path + " is not a file!");
      return;
    }

    try {
      CSVPrinter printer = getCSVBuilder(exportCSV).build().print(new PrintWriter(file));

      if (exportCSV.isExportHeader) {
        printer.printRecord(res.getColumnList());
      }

      while (res.hasMore()) {
        List<List<String>> cache = cacheResult(res, true);
        printer.printRecords(cache);
      }

      printer.flush();
      printer.close();
    } catch (IOException e) {
      System.out.println(
          "Encounter an error when writing csv file " + path + ", because " + e.getMessage());
      return;
    }
    res.close();
    System.out.println("Successfully write csv file: \"" + file.getAbsolutePath() + "\".");
  }

  private static void processLoadCsv(String sql) throws SessionException {
    int CHUNK_SIZE = 1024 * 1024;

    SessionExecuteSqlResult res = session.executeSql(sql);
    String path = res.getLoadCsvPath();

    String parseErrorMsg = res.getParseErrorMsg();
    if (parseErrorMsg != null && !parseErrorMsg.isEmpty()) {
      System.out.println(res.getParseErrorMsg());
      return;
    }

    File file = new File(path);
    if (!file.exists()) {
      System.out.println(path + " does not exist!");
      return;
    }
    if (!file.isFile()) {
      System.out.println(path + " is not a file!");
      return;
    }

    long offset = 0;
    String fileName = System.currentTimeMillis() + ".csv";
    try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
      raf.seek(offset);
      byte[] buffer = new byte[CHUNK_SIZE];
      int bytesRead;
      while ((bytesRead = raf.read(buffer)) != -1) {
        byte[] dataToSend;
        if (bytesRead < CHUNK_SIZE) { // 如果最后一块小于 CHUNK_SIZE，只发送实际读取的部分
          dataToSend = new byte[bytesRead];
          System.arraycopy(buffer, 0, dataToSend, 0, bytesRead);
        } else {
          dataToSend = buffer;
        }
        ByteBuffer data = ByteBuffer.wrap(dataToSend);
        FileChunk chunk = new FileChunk(fileName, offset, data, bytesRead);
        session.uploadFileChunk(chunk);
        offset += bytesRead;
      }
    } catch (IOException e) {
      System.out.println(
          "Encounter an error when reading file " + path + ", because " + e.getMessage());
      return;
    }

    Pair<List<String>, Long> pair = session.executeLoadCSV(sql, fileName);
    List<String> columns = pair.k;
    long recordsNum = pair.v;

    System.out.println("Successfully write " + recordsNum + " record(s) to: " + columns);
  }

  private static String parseExecuteCommand(String[] args) {
    StringBuilder command = new StringBuilder();
    int index = 0;
    for (String arg : args) {
      index++;
      if (arg.equals("-" + EXECUTE_ARGS) || arg.equals("-" + EXECUTE_NAME)) {
        break;
      }
    }
    for (int i = index; i < args.length; i++) {
      if (args[i].startsWith("-")) {
        break;
      }
      command.append(args[i]);
      command.append(" ");
    }
    return command.substring(0, command.toString().length() - 1);
  }

  private static Completer buildIginxCompleter() {
    List<Completer> iginxCompleters = new ArrayList<>();

    List<List<String>> withNullCompleters =
        Arrays.asList(
            Arrays.asList("insert", "into"),
            Arrays.asList("delete", "from"),
            Arrays.asList("delete", "columns"),
            Arrays.asList("explain", "select"),
            Arrays.asList("add", "storageengine"),
            Arrays.asList("create", "function"),
            Arrays.asList("drop", "function"),
            Arrays.asList("commit", "transform", "job"),
            Arrays.asList("show", "transform", "job", "status"),
            Arrays.asList("cancel", "transform", "job"),
            Arrays.asList("set", "time", "unit", "in"),
            Arrays.asList("set", "config"),
            Arrays.asList("show", "config"),
            Arrays.asList("set", "rules"),
            Collections.singletonList("select"));
    addArgumentCompleters(iginxCompleters, withNullCompleters, true);

    List<List<String>> withoutNullCompleters =
        Arrays.asList(
            Arrays.asList("show", "replica", "number"),
            Arrays.asList("count", "points"),
            Arrays.asList("clear", "data"),
            Arrays.asList("show", "columns"),
            Arrays.asList("show", "cluster", "info"),
            Arrays.asList("show", "functions"),
            Arrays.asList("show", "sessionid"),
            Arrays.asList("show", "rules"),
            Arrays.asList("remove", "storageengine"));
    addArgumentCompleters(iginxCompleters, withoutNullCompleters, false);

    List<String> singleCompleters = Arrays.asList("quit", "exit");
    addSingleCompleters(iginxCompleters, singleCompleters);

    Completer iginxCompleter = new AggregateCompleter(iginxCompleters);
    return iginxCompleter;
  }

  private static void addSingleCompleters(
      List<Completer> iginxCompleters, List<String> completers) {
    for (String keyWord : completers) {
      iginxCompleters.add(new StringsCompleter(keyWord.toLowerCase()));
      iginxCompleters.add(new StringsCompleter(keyWord.toUpperCase()));
    }
  }

  private static void addArgumentCompleters(
      List<Completer> iginxCompleters, List<List<String>> completers, boolean needNullCompleter) {
    for (List<String> keyWords : completers) {
      List<Completer> upperCompleters = new ArrayList<>();
      List<Completer> lowerCompleters = new ArrayList<>();

      for (String keyWord : keyWords) {
        upperCompleters.add(new StringsCompleter(keyWord.toUpperCase()));
        lowerCompleters.add(new StringsCompleter(keyWord.toLowerCase()));
      }
      if (needNullCompleter) {
        upperCompleters.add(NullCompleter.INSTANCE);
        lowerCompleters.add(NullCompleter.INSTANCE);
      }

      iginxCompleters.add(new ArgumentCompleter(upperCompleters));
      iginxCompleters.add(new ArgumentCompleter(lowerCompleters));
    }
  }

  public static void echoStarting() {
    System.out.println("-----------------------");
    System.out.println("Starting IGinX Client");
    System.out.println("-----------------------");
  }

  public static void displayLogo(String version) {
    System.out.println(
        "  _____    _____   _          __   __\n"
            + " |_   _|  / ____| (_)         \\ \\ / /\n"
            + "   | |   | |  __   _   _ __    \\ V / \n"
            + "   | |   | | |_ | | | | '_ \\    > <  \n"
            + "  _| |_  | |__| | | | | | | |  / . \\ \n"
            + " |_____|  \\_____| |_| |_| |_| /_/ \\_\\"
            + "     version "
            + version
            + "\n");
  }

  enum OperationResult {
    STOP,
    CONTINUE,
    DO_NOTHING,
  }
}
