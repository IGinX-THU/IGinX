package cn.edu.tsinghua.iginx.integration.expansion.utils;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.DataType;
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
            List<List<Object>> valuesListAns, List<List<Object>> valuesList) {
        for (List<Object> valueListAns : valuesListAns) {
            logger.info(valueListAns.toString());
        }
        for (List<Object> valueList : valuesList) {
            logger.info(valueList.toString());
        }
        Set<List<Object>> valuesSetAns = new HashSet<>(valuesListAns);
        Set<List<Object>> valuesSet = new HashSet<>(valuesList);
        return valuesSet.equals(valuesSetAns);
    }

    public static void executeAndCompare(
            Session session,
            String statement,
            List<String> pathListAns,
            List<List<Object>> valuesListAns,
            List<DataType> dataTypeListAns) {
        try {
            SessionExecuteSqlResult res = session.executeSql(statement);
            List<String> pathList = res.getPaths();
            List<List<Object>> valuesList = res.getValues();
            List<DataType> dataTypeList = res.getDataTypeList();

            logger.info(pathList.toString());
            logger.info(valuesList.toString());
            logger.info(dataTypeList.toString());

            for (int i = 0; i < pathListAns.size(); i++) {
                String pathAns = pathListAns.get(i);
                assertEquals(pathAns, pathList.get(i));
                assertEquals(dataTypeListAns.get(i), dataTypeList.get(i));
            }

            if (!compareValuesList(valuesListAns, valuesList)) {
                logger.error(
                        "actual valuesList is {} and it should be {}", valuesList, valuesListAns);
                fail();
            }
        } catch (SessionException | ExecutionException e) {
            logger.error("Statement: \"{}\" execute fail. Caused by:", statement, e);
            fail();
        }
    }
}
