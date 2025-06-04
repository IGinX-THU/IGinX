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
package cn.edu.tsinghua.iginx.integration.func.udf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.LoadUDFResp;
import cn.edu.tsinghua.iginx.thrift.RegisterTaskInfo;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UDFTestTools {

  private static final Logger LOGGER = LoggerFactory.getLogger(UDFTestTools.class);

  private final Session session;
  private static final String SINGLE_UDF_REGISTER_SQL =
      "CREATE FUNCTION %s \"%s\" FROM \"%s\" IN \"%s\";";

  private static final String MULTI_UDF_REGISTER_SQL = "CREATE FUNCTION %s IN \"%s\";";

  private static final String DROP_SQL = "DROP FUNCTION \"%s\";";

  private static final String SHOW_FUNCTION_SQL = "SHOW FUNCTIONS;";

  public UDFTestTools(Session session) {
    this.session = session;
  }

  void executeReg(String statement) {
    LOGGER.info("Execute {} registration Statement: \"{}\"", "local", statement);
    LoadUDFResp res = null;
    try {
      res = session.executeRegisterTask(statement, false);
    } catch (SessionException e) {
      LOGGER.error("Statement: \"{}\" execute fail. Caused by:", statement, e);
      fail();
    }

    if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
      LOGGER.error(
          "Statement: \"{}\" execute fail. Caused by: {}.", statement, res.getParseErrorMsg());
      fail();
    }
  }

  // register UDF and expect failure
  void executeRegFail(String statement) {
    LOGGER.info("Execute {} registration Statement: \"{}\"", "local", statement);
    LoadUDFResp res = null;
    try {
      res = session.executeRegisterTask(statement, false);
    } catch (SessionException e) {
      // don't want to print e because it will be confusing
      LOGGER.info(
          "Statement: \"{}\" execute failed AS EXPECTED, with message: {}",
          statement,
          e.getMessage());
      return;
    }

    if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
      LOGGER.info(
          "Statement: \"{}\" execute failed AS EXPECTED, with message: {}.",
          statement,
          res.getParseErrorMsg());
      return;
    }

    fail("Statement: \"" + statement + "\" execute without failure, which was not expected.");
  }

  SessionExecuteSqlResult execute(String statement) {
    LOGGER.info("Execute Statement: \"{}\"", statement);

    SessionExecuteSqlResult res = null;
    try {
      res = session.executeSql(statement);
    } catch (SessionException e) {
      LOGGER.error("Statement: \"{}\" execute fail. Caused by:", statement, e);
      fail();
    }

    if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
      LOGGER.error(
          "Statement: \"{}\" execute fail. Caused by: {}.", statement, res.getParseErrorMsg());
      fail();
    }

    return res;
  }

  void dropTasks(List<String> names) {
    LOGGER.info("Drop Tasks: names={}", names);
    SessionExecuteSqlResult res;
    try {
      res = session.executeSql(SHOW_FUNCTION_SQL);
      for (RegisterTaskInfo info : res.getRegisterTaskInfos()) {
        for (String name : names) {
          if (info.getName().equals(name)) {
            session.executeSql(String.format(DROP_SQL, name));
          }
        }
      }
    } catch (SessionException e) {
      LOGGER.error("Execution failed.", e);
    }
  }

  // execute a statement and expect failure.
  Exception executeFail(String statement) {
    LOGGER.info("Execute Statement: \"{}\"", statement);

    SessionExecuteSqlResult res = null;
    try {
      res = session.executeSql(statement);
    } catch (SessionException e) {
      // don't want to print e because it will be confusing
      LOGGER.info(
          "Statement: \"{}\" execute failed AS EXPECTED, with message: {}",
          statement,
          e.getMessage());
      return e;
    }

    if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
      LOGGER.info(
          "Statement: \"{}\" execute failed AS EXPECTED, with message: {}.",
          statement,
          res.getParseErrorMsg());
      return new SessionException(res.getParseErrorMsg());
    }

    fail("Statement: \"" + statement + "\" execute without failure, which was not expected.");
    throw new AssertionError(
        "Statement executed successfully when failure was expected: " + statement);
  }

  public void executeAndCompareErrMsg(String statement, String expectedErrMsg) {
    LOGGER.info("Execute Statement: \"{}\"", statement);

    try {
      session.executeSql(statement);
    } catch (SessionException e) {
      LOGGER.info("Statement: \"{}\" execute fail. Because: ", statement, e);
      assertEquals(expectedErrMsg, e.getMessage());
    }
  }

  public boolean isUDFRegistered(String udfName) {
    SessionExecuteSqlResult ret = execute(SHOW_FUNCTION_SQL);
    List<String> registerUDFs =
        ret.getRegisterTaskInfos().stream()
            .map(RegisterTaskInfo::getName)
            .collect(Collectors.toList());
    return registerUDFs.contains(udfName);
  }

  public boolean isUDFsRegistered(List<String> names) {
    SessionExecuteSqlResult ret = execute(SHOW_FUNCTION_SQL);
    List<String> registerUDFs =
        ret.getRegisterTaskInfos().stream()
            .map(RegisterTaskInfo::getName)
            .collect(Collectors.toList());
    for (String udfName : names) {
      if (!registerUDFs.contains(udfName)) {
        return false;
      }
    }
    return true;
  }

  // all udf shouldn't be registered.
  boolean isUDFsUnregistered(List<String> names) {
    SessionExecuteSqlResult ret = execute(SHOW_FUNCTION_SQL);
    List<String> registerUDFs =
        ret.getRegisterTaskInfos().stream()
            .map(RegisterTaskInfo::getName)
            .collect(Collectors.toList());
    for (String udfName : names) {
      if (registerUDFs.contains(udfName)) {
        return false;
      }
    }
    return true;
  }

  /**
   * generate multiple UDFs' registration sql command
   *
   * @param types UDF types
   * @param names UDF names that will be used in sql after
   * @param classPaths UDF class path relative to module root
   * @param modulePath module dir position
   * @return a sql string
   */
  String concatMultiUDFReg(
      List<String> types, List<String> names, List<String> classPaths, String modulePath) {
    assertEquals(types.size(), names.size());
    assertEquals(names.size(), classPaths.size());
    String udfs =
        IntStream.range(0, types.size())
            .mapToObj(
                i ->
                    String.format(
                        "%s \"%s\" FROM \"%s\"", types.get(i), names.get(i), classPaths.get(i)))
            .collect(Collectors.joining(", "));
    return String.format(MULTI_UDF_REGISTER_SQL, udfs, modulePath);
  }

  // all UDFs will be registered in one type
  String concatMultiUDFReg(
      String type, List<String> names, List<String> classPaths, String modulePath) {
    assertEquals(names.size(), classPaths.size());
    String udfs =
        IntStream.range(0, names.size())
            .mapToObj(i -> String.format("\"%s\" FROM \"%s\"", names.get(i), classPaths.get(i)))
            .collect(Collectors.joining(", "));
    return String.format(MULTI_UDF_REGISTER_SQL, type + " " + udfs, modulePath);
  }
}
