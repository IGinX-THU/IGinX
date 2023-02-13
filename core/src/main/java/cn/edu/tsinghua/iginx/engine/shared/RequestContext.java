package cn.edu.tsinghua.iginx.engine.shared;

import cn.edu.tsinghua.iginx.sql.statement.Statement;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.utils.SnowFlakeUtils;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

@Data
public class RequestContext {

    private long id;

    private long startTime;

    private long endTime;

    private transient long sessionId;

    private Map<String, Object> extraParams;

    private transient Status status;

    private String sql;

    private boolean fromSQL;

    private SqlType sqlType;

    private transient Statement statement;

    private transient Result result;

    private boolean useStream;

    private boolean recover;

    private boolean enableFaultTolerance;

    private transient CountDownLatch resultLatch = new CountDownLatch(1);

    private void init() {
        this.id = SnowFlakeUtils.getInstance().nextId();
        this.startTime = System.currentTimeMillis();
        this.extraParams = new HashMap<>();
    }

    public RequestContext(long sessionId) {
        init();
        this.sessionId = sessionId;
    }

    public RequestContext() {
        this.extraParams = new HashMap<>();
    }

    public RequestContext(long sessionId, Statement statement) {
        this(sessionId, statement, false);
    }

    public RequestContext(long sessionId, Statement statement, boolean useStream) {
        init();
        this.sessionId = sessionId;
        this.statement = statement;
        this.fromSQL = false;
        this.useStream = useStream;
    }

    public RequestContext(long sessionId, String sql) {
        this(sessionId, sql, false);
    }

    public RequestContext(long sessionId, String sql, boolean useStream) {
        init();
        this.sessionId = sessionId;
        this.sql = sql;
        this.fromSQL = true;
        this.sqlType = SqlType.Unknown;
        this.useStream = useStream;
        this.recover = false;
    }

    public RequestContext(long sessionId, long queryId, String sql, boolean useStream) {
        this.sessionId = sessionId;
        this.id = queryId;
        this.sql = sql;
        this.fromSQL = true;
        this.sqlType = SqlType.Unknown;
        this.useStream = useStream;
        this.recover = true;

        this.startTime = System.currentTimeMillis();
        this.extraParams = new HashMap<>();
    }

    public Object getExtraParam(String key) {
        return extraParams.getOrDefault(key, null);
    }

    public void setExtraParam(String key, Object value) {
        extraParams.put(key, value);
    }

    public boolean isUseStream() {
        return useStream;
    }

    public void setResult(Result result) {
        this.result = result;
        if (this.result != null) {
            this.result.setQueryId(id);
        }
        this.resultLatch.countDown();
        this.endTime = System.currentTimeMillis();
    }

    public Result takeResult() {
        try {
            resultLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return this.result;
    }
}
