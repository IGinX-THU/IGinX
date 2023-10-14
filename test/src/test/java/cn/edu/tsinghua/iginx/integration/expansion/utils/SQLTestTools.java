package cn.edu.tsinghua.iginx.integration.expansion.utils;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLTestTools {

  private static final Logger logger = LoggerFactory.getLogger(SQLTestTools.class);

  public static void executeAndCompare(Session session, String statement, String exceptOutput) {
    String actualOutput = execute(session, statement);
    assertEquals(exceptOutput, actualOutput);
  }

  private static String execute(Session session, String statement) {
    logger.info("Execute Statement: \"{}\"", statement);

    SessionExecuteSqlResult res = null;
    try {
      res = session.executeSql(statement);
    } catch (SessionException | ExecutionException e) {
      logger.error("Statement: \"{}\" execute fail. Caused by:", statement, e);
      fail();
    }

    if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
      logger.error(
          "Statement: \"{}\" execute fail. Caused by: {}.", statement, res.getParseErrorMsg());
      fail();
      return "";
    }

    return res.getResultInString(false, "");
  }

  private static void compareValuesList(
      List<List<Object>> expectedValuesList, List<List<Object>> actualValuesList) {
    Set<List<String>> expectedSet =
        expectedValuesList.stream()
            .map(
                row -> {
                  List<String> strValues = new ArrayList<>();
                  row.forEach(
                      val -> {
                        if (val instanceof byte[]) {
                          strValues.add(new String((byte[]) val));
                        } else {
                          strValues.add(String.valueOf(val));
                        }
                      });
                  return strValues;
                })
            .collect(Collectors.toSet());

    Set<List<String>> actualSet =
        actualValuesList.stream()
            .map(
                row -> {
                  List<String> strValues = new ArrayList<>();
                  row.forEach(
                      val -> {
                        if (val instanceof byte[]) {
                          strValues.add(new String((byte[]) val));
                        } else {
                          strValues.add(String.valueOf(val));
                        }
                      });
                  return strValues;
                })
            .collect(Collectors.toSet());

    if (!expectedSet.equals(actualSet)) {
      logger.error("actual valuesList is {} and it should be {}", actualSet, expectedSet);
      fail();
    }
  }

  public static void executeAndCompare(
      Session session,
      String statement,
      List<String> pathListAns,
      List<List<Object>> expectedValuesList) {
    try {
      SessionExecuteSqlResult res = session.executeSql(statement);
      List<String> pathList = res.getPaths();
      List<List<Object>> actualValuesList = res.getValues();

      assertArrayEquals(new List[] {pathListAns}, new List[] {pathList});

      compareValuesList(expectedValuesList, actualValuesList);
    } catch (SessionException | ExecutionException e) {
      logger.error("Statement: \"{}\" execute fail. Caused by:", statement, e);
      fail();
    }
  }
}
