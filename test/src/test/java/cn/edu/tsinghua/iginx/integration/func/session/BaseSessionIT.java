package cn.edu.tsinghua.iginx.integration.func.session;

import static cn.edu.tsinghua.iginx.integration.controller.Controller.CLEAR_DATA_EXCEPTION;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import cn.edu.tsinghua.iginx.integration.tool.MultiConnection;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseSessionIT {

    private static final Logger logger = LoggerFactory.getLogger(BaseSessionIT.class);

    // parameters to be flexibly configured by inheritance
    protected static MultiConnection session;

    // host info
    protected String defaultTestHost = "127.0.0.1";
    protected int defaultTestPort = 6888;
    protected String defaultTestUser = "root";
    protected String defaultTestPass = "root";

    protected boolean isAbleToDelete;

    protected static final double delta = 1e-7;
    protected static final long TIME_PERIOD = 100000L;
    protected static final long START_TIME = 1000L;
    protected static final long END_TIME = START_TIME + TIME_PERIOD - 1;
    // params for partialDelete
    protected long delStartTime = START_TIME + TIME_PERIOD / 5;
    protected long delEndTime = START_TIME + TIME_PERIOD / 10 * 9;
    protected long delTimePeriod = delEndTime - delStartTime;
    protected double deleteAvg =
            ((START_TIME + END_TIME) * TIME_PERIOD / 2.0
                            - (delStartTime + delEndTime - 1) * delTimePeriod / 2.0)
                    / (TIME_PERIOD - delTimePeriod);

    protected int currPath = 0;

    protected BaseSessionIT() {
        ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
        DBConf dbConf = conf.loadDBConf(conf.getStorageType());
        this.isAbleToDelete = dbConf.getEnumValue(DBConf.DBConfType.isAbleToDelete);
    }

    @Before
    public void setUp() {
        try {
            session =
                    new MultiConnection(
                            new Session(
                                    defaultTestHost,
                                    defaultTestPort,
                                    defaultTestUser,
                                    defaultTestPass));
            session.openSession();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    @After
    public void tearDown() {
        try {
            clearData();
            session.closeSession();
        } catch (SessionException e) {
            logger.error(e.getMessage());
        }
    }

    protected void clearData() throws SessionException {
        if (session.isClosed()) {
            session =
                    new MultiConnection(
                            new Session(
                                    defaultTestHost,
                                    defaultTestPort,
                                    defaultTestUser,
                                    defaultTestPass));
            session.openSession();
        }

        Controller.clearData(session);
    }

    protected int getPathNum(String path) {
        if (path.contains("(") && path.contains(")")) {
            path = path.substring(path.indexOf("(") + 1, path.indexOf(")"));
        }

        String pattern = "^sg1\\.d(\\d+)\\.s(\\d+)$";
        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(path);
        if (m.find()) {
            int d = Integer.parseInt(m.group(1));
            int s = Integer.parseInt(m.group(2));
            if (d == s) {
                return d;
            } else {
                return -1;
            }
        } else {
            return -1;
        }
    }

    protected List<String> getPaths(int startPosition, int len) {
        List<String> paths = new ArrayList<>();
        for (int i = startPosition; i < startPosition + len; i++) {
            paths.add("sg1.d" + i + ".s" + i);
        }
        return paths;
    }

    protected void insertNumRecords(List<String> insertPaths)
            throws SessionException, ExecutionException {
        int pathLen = insertPaths.size();
        long[] timestamps = new long[(int) TIME_PERIOD];
        for (long i = 0; i < TIME_PERIOD; i++) {
            timestamps[(int) i] = i + START_TIME;
        }

        Object[] valuesList = new Object[pathLen];
        for (int i = 0; i < pathLen; i++) {
            int pathNum = getPathNum(insertPaths.get(i));
            Object[] values = new Object[(int) TIME_PERIOD];
            for (long j = 0; j < TIME_PERIOD; j++) {
                values[(int) j] = pathNum + j + START_TIME;
            }
            valuesList[i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < pathLen; i++) {
            dataTypeList.add(DataType.LONG);
        }
        session.insertNonAlignedColumnRecords(
                insertPaths, timestamps, valuesList, dataTypeList, null);
    }

    protected double changeResultToDouble(Object rawResult) {
        double result = 0;
        if (rawResult instanceof java.lang.Long) {
            result = (double) ((long) rawResult);
        } else {
            try {
                result = (double) rawResult;
            } catch (Exception e) {
                logger.error(e.getMessage());
                fail();
            }
        }
        return result;
    }
}
