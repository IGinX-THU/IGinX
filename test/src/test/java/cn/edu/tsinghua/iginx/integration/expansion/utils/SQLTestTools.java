package cn.edu.tsinghua.iginx.integration.expansion.utils;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import java.util.*;
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
                    "Statement: \"{}\" execute fail. Caused by: {}.",
                    statement,
                    res.getParseErrorMsg());
            fail();
            return "";
        }

        return res.getResultInString(false, "");
    }

    private static boolean compareValuesList(
            List<List<Object>> expectedValuesList, List<List<Object>> actualValuesList) {
        if (expectedValuesList.size() != actualValuesList.size()) {
            return false;
        }

        for (int i = 0; i < expectedValuesList.size(); i++) {
            List<Object> rowA = expectedValuesList.get(i);
            List<Object> rowB = actualValuesList.get(i);
            if (rowA.size() != rowB.size()) {
                return false;
            }

            for (int j = 0; j < rowA.size(); j++) {
                String valueA = String.valueOf(rowA.get(j));
                String valueB;
                if (rowB.get(i) instanceof byte[]) {
                    valueB = new String((byte[]) rowB.get(i));
                } else {
                    valueB = String.valueOf(rowB.get(j));
                }
                if (!valueA.equals(valueB)) {
                    return false;
                }
            }
        }
        return true;
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

            for (int i = 0; i < pathListAns.size(); i++) {
                assertEquals(pathListAns.get(i), pathList.get(i));
            }

            if (!compareValuesList(expectedValuesList, actualValuesList)) {
                logger.error(
                        "actual valuesList is {} and it should be {}",
                        actualValuesList,
                        expectedValuesList);
                fail();
            }
        } catch (SessionException | ExecutionException e) {
            logger.error("Statement: \"{}\" execute fail. Caused by:", statement, e);
            fail();
        }
    }
}
