package cn.edu.tsinghua.iginx.integration.expansion.unit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    private static boolean compareList(List<List<Object>> valAns, List<List<Object>> val) {

        Map<Object, Integer> map1 = new HashMap<>();
        Map<Object, Integer> map2 = new HashMap<>();

        for (List<Object> list : valAns) {
            for (Object o : list) {
                if (o != null) map1.put(o, map1.getOrDefault(o, 0) + 1);
            }
        }

        for (List<Object> list : val) {
            for (Object o : list) {
                if (o != null) map2.put(o, map2.getOrDefault(o, 0) + 1);
            }
        }

        return map1.equals(map2);
    }

    public static void executeAndCompare(
            Session session,
            String statement,
            List<String> headerAns,
            List<List<Object>> valAns,
            List<DataType> dataTypeAns) {
        SessionExecuteSqlResult res = null;
        try {
            res = session.executeSql(statement);
        } catch (SessionException | ExecutionException e) {
            logger.error("Statement: \"{}\" execute fail. Caused by:", statement, e);
            fail();
        }
        int len = valAns.get(0).size();
        List<String> headerVal = res.getPaths();
        List<List<Object>> val = res.getValues();
        List<DataType> dataTypes = res.getDataTypeList();

        if (headerAns.contains(headerVal) && headerAns.size() == headerVal.size()) {
            logger.error("header result not right with ans {} and it should be {}", val, valAns);
            fail();
        }

        if (dataTypeAns.contains(dataTypes) && dataTypeAns.size() == dataTypes.size()) {
            logger.error(
                    "dataType result not right with ans {} and it should be {}",
                    dataTypes,
                    dataTypeAns);
            fail();
        }

        if (!compareList(val, valAns)) {
            logger.error("val result not right with ans {} and it should be {}", val, valAns);
            fail();
        }
    }
}
