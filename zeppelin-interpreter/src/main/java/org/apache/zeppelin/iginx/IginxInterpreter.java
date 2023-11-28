package org.apache.zeppelin.iginx;

import static cn.edu.tsinghua.iginx.utils.FileUtils.exportByteStream;

import cn.edu.tsinghua.iginx.constant.GlobalConstant;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.QueryDataSet;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.utils.FormatUtils;
import java.io.*;
import java.nio.file.*;
import java.security.InvalidParameterException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.zeppelin.interpreter.*;

public class IginxInterpreter extends AbstractInterpreter {

  private static final String IGINX_HOST = "iginx.host";
  private static final String IGINX_PORT = "iginx.port";
  private static final String IGINX_USERNAME = "iginx.username";
  private static final String IGINX_PASSWORD = "iginx.password";
  private static final String IGINX_TIME_PRECISION = "iginx.time.precision";
  private static final String IGINX_OUTFILE_DIR = "iginx.outfile.dir";
  private static final String IGINX_FETCH_SIZE = "iginx.fetch.size";
  private static final String IGINX_OUTFILE_MAX_NUM = "iginx.outfile.max.num";
  private static final String IGINX_OUTFILE_MAX_SIZE = "iginx.outfile.max.size";

  private static final String DEFAULT_HOST = "127.0.0.1";
  private static final String DEFAULT_PORT = "6888";
  private static final String DEFAULT_USERNAME = "root";
  private static final String DEFAULT_PASSWORD = "root";
  private static final String DEFAULT_TIME_PRECISION = "ns";
  private static final String DEFAULT_OUTFILE_DIR = "/your/outfile/path";
  private static final String DEFAULT_FETCH_SIZE = "1000";
  private static final String DEFAULT_OUTFILE_MAX_NUM = "100";
  private static final String DEFAULT_OUTFILE_MAX_SIZE = "10240";

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
  private String outfileDir = "";
  private String fetchSize = "";
  private int outfileMaxNum = 0;
  private int outfileMaxSize = 0;

  private Queue<String> downloadFileQueue = new LinkedList<>();
  private Queue<Double> downloadFileSizeQueue = new LinkedList<>();
  private double downloadFileTotalSize = 0L;

  private String outfileRegex = "(?i)(\\bINTO\\s+OUTFILE\\s+\")(.*?)(\"\\s+AS\\s+STREAM\\b)";


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
    outfileDir = properties.getProperty(IGINX_OUTFILE_DIR, DEFAULT_OUTFILE_DIR).trim();
    fetchSize = properties.getProperty(IGINX_FETCH_SIZE, DEFAULT_FETCH_SIZE).trim();
    outfileMaxNum =
        Integer.parseInt(
            properties.getProperty(IGINX_OUTFILE_MAX_NUM, DEFAULT_OUTFILE_MAX_NUM).trim());
    outfileMaxSize =
        Integer.parseInt(
            properties.getProperty(IGINX_OUTFILE_MAX_SIZE, DEFAULT_OUTFILE_MAX_SIZE).trim());

    session = new Session(host, port, username, password);
    try {
      session.openSession();
    } catch (SessionException e) {
      exception = e;
      System.out.println("Can not open session successfully.");
    }

    try {
      loadNGINXStaticFilesInfo();
    } catch (IOException e) {
      throw new RuntimeException(e);
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
      if (isSessionClosedError(interpreterResult)) {
        if (reopenSession()) {
          interpreterResult = processSql(cmd);
        } else {
          interpreterResult.add(
              InterpreterResult.Type.TEXT,
              "Can not reopen session successfully, please check IGinX Server.");
        }
      }
    }

    return interpreterResult;
  }

  private InterpreterResult processSql(String sql) {
    try {
      // 如果sql中有outfile关键字，则进行特殊处理，将结果下载到zeppelin所在的服务器上，并在表单中返回下载链接
      String outfileRegex = "(?i)(\\bINTO\\s+OUTFILE\\s+\")(.*?)(\"\\s+AS\\s+STREAM\\b)";
      Pattern pattern = Pattern.compile(outfileRegex);
      Matcher matcher = pattern.matcher(sql.toLowerCase());
      if (matcher.find()) {
        return processOutfileSql(sql, matcher.group(1));
      }

      SessionExecuteSqlResult sqlResult = session.executeSql(sql);

      String parseErrorMsg = sqlResult.getParseErrorMsg();
      if (parseErrorMsg != null && !parseErrorMsg.equals("")) {
        return new InterpreterResult(InterpreterResult.Code.ERROR, sqlResult.getParseErrorMsg());
      }

      InterpreterResult interpreterResult;
      String msg;

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

  /**
   * 处理带有outfile关键字的sql语句，将结果下载到zeppelin所在的服务器上，并在表单中返回下载链接
   *
   * @param sql 带有outfile关键字的sql语句
   * @param originOutfilePath 原始的outfile路径
   * @return InterpreterResult
   * @throws SessionException
   * @throws ExecutionException
   * @throws IOException
   */
  private InterpreterResult processOutfileSql(String sql, String originOutfilePath)
      throws SessionException, ExecutionException, IOException {

    // 根据当前年月日时分秒毫秒生成outfile的文件夹名，将文件下载到此处
    String dateDir = new Date().toString().replace(" ", "-").replace(":", "-");
    String outfileDirPath = Paths.get(outfileDir, dateDir).toString();

    // 替换sql中最后一个outfile关键词，替换文件路径为Zeppelin在服务端指定的路径
    Pattern pattern = Pattern.compile(outfileRegex);
    Matcher matcher = pattern.matcher(sql);
    if(matcher.find()){
      int lastMatchEnd = 0;
      while (matcher.find()) {
        lastMatchEnd = matcher.end(); // 记录匹配结束的位置
      }

      // 替换最后一个匹配的路径
      sql = sql.substring(0, lastMatchEnd) +
              sql.substring(lastMatchEnd).replaceFirst(outfileRegex, "$1" + outfileDirPath + "$3");

    }


    QueryDataSet res = session.executeQuery(sql);

    processExportByteStream(res);

    // 获取outfileDirPath文件夹下的所有文件名，只有一级，不需要递归
    File outfileFolder = new File(outfileDirPath);
    String[] fileNames = outfileFolder.list();

    // 如果有多个文件，压缩outfileDirPath文件夹
    boolean hasMultipleFiles = fileNames != null && fileNames.length > 1;
    String zipName = "all_file.zip";
    if (hasMultipleFiles) {
      FileOutputStream outputStream =
          new FileOutputStream(Paths.get(outfileDirPath, zipName).toString());
      ArrayList<File> fileList = new ArrayList<>();
      for (String fileName : fileNames) {
        fileList.add(new File(outfileDirPath + "/" + fileName));
      }
      toZip(fileList, outputStream);
      outputStream.close();
    }

    // 清理NGINX_STATIC文件夹
    downloadFileQueue.add(outfileDirPath);
    double fileSize = getFileSize(outfileDirPath);
    downloadFileSizeQueue.add(fileSize);
    downloadFileTotalSize += fileSize;
    clearNGINXStaticFiles();

    // 构建表格
    String downloadLink = "%%html<a href=\"%s\" download=\"%s\">点击下载</a>";
    StringBuilder builder = new StringBuilder();
    builder.append("文件名").append(TAB).append("下载链接").append(NEWLINE);
    if (hasMultipleFiles) {
      builder
          .append("所有文件压缩包")
          .append(TAB)
          .append(String.format(downloadLink, Paths.get("static", dateDir, zipName), zipName))
          .append(NEWLINE);
    }
    if (fileNames != null) {
      for (String fileName : fileNames) {
        builder
            .append(fileName)
            .append(TAB)
            .append(String.format(downloadLink, Paths.get("static", dateDir, fileName), fileName))
            .append(NEWLINE);
      }
    }
    String msg = builder.toString();
    InterpreterResult interpreterResult = new InterpreterResult(InterpreterResult.Code.SUCCESS);
    interpreterResult.add(InterpreterResult.Type.TABLE, msg);

    return interpreterResult;
  }

  /**
   * 将QueryDataSet中的结果导出到文件中。 拷贝自Client模块的Outfile相关代码，因为Client模块不能被引用
   *
   * @param res QueryDataSet
   * @throws SessionException
   * @throws ExecutionException
   * @throws IOException
   */
  private void processExportByteStream(QueryDataSet res)
      throws SessionException, ExecutionException, IOException {
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
      // 将文件名中的反斜杠\替换为.，因为web路径不能识别\
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

  /**
   * 将QueryDataSet中的结果缓存到List<List<byte[]>>中，每一行为一个List<byte[]>，每一列为一个byte[]
   * 拷贝自Client模块的Outfile相关代码，因为Client模块不能被引用
   *
   * @param queryDataSet QueryDataSet
   * @return 缓存结果
   * @throws SessionException
   * @throws ExecutionException
   */
  private List<List<byte[]>> cacheResultByteArray(QueryDataSet queryDataSet)
      throws SessionException, ExecutionException {
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

  /**
   * 将给定的文件列表压缩成zip文件，输出到给定的输出流中
   *
   * @param srcFiles 文件列表
   * @param out 输出流
   * @throws RuntimeException
   */
  public static void toZip(List<File> srcFiles, OutputStream out) throws RuntimeException {
    int BUFFER_SIZE = 2 * 1024;
    ZipOutputStream zos = null;
    try {
      zos = new ZipOutputStream(out);
      for (File srcFile : srcFiles) {
        byte[] buf = new byte[BUFFER_SIZE];
        zos.putNextEntry(new ZipEntry(srcFile.getName()));
        int len;
        FileInputStream in = new FileInputStream(srcFile);
        while ((len = in.read(buf)) != -1) {
          zos.write(buf, 0, len);
        }
        zos.closeEntry();
        in.close();
      }
    } catch (Exception e) {
      throw new RuntimeException("zip error", e);
    } finally {
      if (zos != null) {
        try {
          zos.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  /** 加载NGINX_STATIC文件夹下的文件信息，将文件夹名和文件夹大小加入到downloadFileQueue和downloadFileSizeQueue中 */
  private void loadNGINXStaticFilesInfo() throws IOException {
    File nginxStaticFolder = new File(outfileDir);
    if (!nginxStaticFolder.exists() || !nginxStaticFolder.isDirectory()) {
      return;
    }
    File[] nginxStaticFiles = nginxStaticFolder.listFiles();
    if (nginxStaticFiles == null) {
      return;
    }
    for (File nginxStaticFile : nginxStaticFiles) {
      if (nginxStaticFile.isDirectory()) {
        downloadFileQueue.add(nginxStaticFile.getAbsolutePath());
        downloadFileSizeQueue.add(getFileSize(nginxStaticFile.getAbsolutePath()));
        downloadFileTotalSize += getFileSize(nginxStaticFile.getAbsolutePath());
      }
    }
  }

  /**
   * 清理NGINX_STATIC文件夹，如果NGINX_STATIC文件夹中的文件夹数量或大小超过一定数量，清理掉最早的文件夹，直到文件夹数量和大小小于一定数量
   * 不清除当前下载的文件夹（即最新的文件夹）
   */
  private void clearNGINXStaticFiles() throws IOException {
    File nginxStaticFolder = new File(outfileDir);
    if (!nginxStaticFolder.exists() || !nginxStaticFolder.isDirectory()) {
      return;
    }
    // 检查NGINX_STATIC文件夹下的文件夹数量和大小
    while ((downloadFileQueue.size() > outfileMaxNum || downloadFileTotalSize > outfileMaxSize)
        && downloadFileQueue.size() > 1) {
      String oldestFolder = downloadFileQueue.poll();
      double oldestFileSize = downloadFileSizeQueue.poll();
      downloadFileTotalSize -= oldestFileSize;
      if (oldestFolder != null) {
        Files.walkFileTree(
            Paths.get(oldestFolder),
            new SimpleFileVisitor<Path>() {
              @Override
              public FileVisitResult visitFile(
                  Path file, java.nio.file.attribute.BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
              }

              @Override
              public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                  throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
              }
            });
      }
    }
  }

  /**
   * 获取文件夹下所有文件的大小(MB)
   *
   * @param path 文件夹路径
   * @return 文件夹下所有文件的大小(MB)
   * @throws IOException
   */
  private double getFileSize(String path) throws IOException {
    final double[] fileSize = {0L};
    Files.walkFileTree(
        Paths.get(path),
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(
              Path file, java.nio.file.attribute.BasicFileAttributes attrs) throws IOException {
            fileSize[0] += Files.size(file) / 1024.0 / 1024.0;
            return FileVisitResult.CONTINUE;
          }
        });

    return fileSize[0];
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
            .split("(?<=;)");
    return Arrays.stream(tmp).map(String::trim).toArray(String[]::new);
  }

  private String convertToHTMLString(String str) {
    return "%html" + str.replace("\n", "<br>").replace("\t", "&nbsp;&nbsp;&nbsp;&nbsp;");
  }

  @Override
  public void cancel(InterpreterContext context) throws InterpreterException {}

  @Override
  public FormType getFormType() throws InterpreterException {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) throws InterpreterException {
    return 0;
  }

  /**
   * 根据InterpreterResult返回的错误信息，判断是不是session关闭导致的错误
   *
   * @param interpreterResult InterpreterResult
   * @return true表示是session关闭导致的错误，false表示不是
   */
  private boolean isSessionClosedError(InterpreterResult interpreterResult) {
    if (interpreterResult.code() != InterpreterResult.Code.ERROR) {
      return false;
    }

    for (InterpreterResultMessage interpreterResultMessage : interpreterResult.message()) {
      String msg = interpreterResultMessage.getData();
      if (msg.contains("org.apache.thrift.transport.TTransportException")) {
        return true;
      }
    }

    return false;
  }

  private boolean reopenSession() {
    try {
      session.closeSession();
      session.openSession();
    } catch (SessionException e) {
      return false;
    }
    return true;
  }
}
