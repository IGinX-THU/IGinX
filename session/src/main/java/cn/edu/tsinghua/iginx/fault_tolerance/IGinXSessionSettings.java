package cn.edu.tsinghua.iginx.fault_tolerance;

import cn.edu.tsinghua.iginx.thrift.EndPoint;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class IGinXSessionSettings {

    private static final boolean DEFAULT_ENABLE_RETRY = true;

    private static final int DEFAULT_MAX_RETRY_TIMES = 2;

    private static final String DEFAULT_USERNAME = "root";

    private static final String DEFAULT_PASSWORD = "root";

    private static final boolean DEFAULT_ENABLE_HIGH_AVAILABLE = true;

    private static final boolean DEFAULT_AUTO_LOAD_AVAILABLE_LIST = false;

    private static final int DEFAULT_MAX_SWITCH_TIMES = 3;

    private static final long DEFAULT_LOAD_AVAILABLE_LIST_INTERVAL = 30;

    private static final TimeUnit DEFAULT_LOAD_AVAILABLE_LIST_INTERVAL_Unit = TimeUnit.SECONDS;


    private static final int DEFAULT_SOCKET_TIMEOUT = 0;

    private static final int DEFAULT_CONNECTION_TIMEOUT = 0;


    private IGinXEndPoint[] endPoints;

    private int endPointsIndex;

    private int endPointsStartIndex;

    private boolean hasMoreEndPoint;

    private final boolean enableRetry;

    private final int maxRetryTimes;

    private final String username;

    private final String password;

    private final boolean enableHighAvailable;

    private final boolean autoLoadAvailableList;

    private final int maxSwitchTimes;

    private final long loadAvailableListInterval;

    private final TimeUnit loadAvailableListIntervalUnit;

    private final int socketTimeout;

    private final int connectionTimeout;

    public IGinXSessionSettings(IGinXEndPoint[] endPoints, boolean enableRetry, int maxRetryTimes, String username,
                                String password, boolean enableHighAvailable, boolean autoLoadAvailableList,
                                int maxSwitchTimes, long loadAvailableListInterval, TimeUnit loadAvailableListIntervalUnit,
                                int socketTimeout, int connectionTimeout) {
        this.endPoints = endPoints;
        this.username = username;
        this.password = password;

        this.enableRetry = enableRetry;
        if (this.enableRetry) {
            this.maxRetryTimes = maxRetryTimes;
        } else {
            this.maxRetryTimes = 0;
        }

        this.enableHighAvailable = enableHighAvailable;
        if (this.enableHighAvailable) {
            this.maxSwitchTimes = maxSwitchTimes;
            this.autoLoadAvailableList = autoLoadAvailableList;
            if (this.autoLoadAvailableList) {
                this.loadAvailableListInterval = loadAvailableListInterval;
                this.loadAvailableListIntervalUnit = loadAvailableListIntervalUnit;
            } else {
                this.loadAvailableListInterval = 0L;
                this.loadAvailableListIntervalUnit = TimeUnit.SECONDS;
            }
        } else {
            this.autoLoadAvailableList = false;
            this.maxSwitchTimes = 0;
            this.loadAvailableListInterval = 0L;
            this.loadAvailableListIntervalUnit = TimeUnit.SECONDS;
        }

        this.endPointsIndex = 0;
        this.endPointsStartIndex = 0;
        this.hasMoreEndPoint = true;

        this.socketTimeout = socketTimeout;
        this.connectionTimeout = connectionTimeout;
    }

    public IGinXEndPoint[] getEndPoints() {
        return endPoints;
    }

    synchronized boolean hasMoreEndPoint() {
        return hasMoreEndPoint;
    }

    synchronized IGinXEndPoint getEndPoint() {
        IGinXEndPoint endPoint = this.endPoints[this.endPointsIndex];
        this.endPointsIndex = (this.endPointsIndex + 1) % this.endPoints.length;
        if (this.endPointsIndex == this.endPointsStartIndex) {
            this.hasMoreEndPoint = false;
        }
        return endPoint;
    }

    synchronized void resetEndPointsIndex() {
        this.endPointsStartIndex = this.endPointsIndex;
        this.hasMoreEndPoint = true;
    }

    synchronized void setEndPoints(List<EndPoint> endPointList) {
        IGinXEndPoint[] endPoints = new IGinXEndPoint[endPointList.size()];
        for (int i = 0; i < endPointList.size(); i++) {
            EndPoint endPoint = endPointList.get(i);
            endPoints[i] = new IGinXEndPoint(endPoint.getIp(), endPoint.getPort());
        }
        this.endPoints = endPoints;
        this.endPointsIndex = 0;
        this.hasMoreEndPoint = true;
    }

    public boolean isEnableRetry() {
        return enableRetry;
    }

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public boolean isEnableHighAvailable() {
        return enableHighAvailable;
    }

    public boolean isAutoLoadAvailableList() {
        return autoLoadAvailableList;
    }

    public int getMaxSwitchTimes() {
        return maxSwitchTimes;
    }

    public long getLoadAvailableListInterval() {
        return loadAvailableListInterval;
    }

    public TimeUnit getLoadAvailableListIntervalUnit() {
        return loadAvailableListIntervalUnit;
    }

    public static class Builder {

        private String connectionString;

        private boolean enableRetry;

        private int maxRetryTimes;

        private String username;

        private String password;

        private boolean enableHighAvailable;

        private boolean autoLoadAvailableList;

        private int maxSwitchTimes;

        private long loadAvailableListInterval;

        private TimeUnit loadAvailableListIntervalUnit;

        private int socketTimeout;

        private int connectionTimeout;

        public Builder() {
            this.connectionString = null;
            this.enableRetry = DEFAULT_ENABLE_RETRY;
            this.maxRetryTimes = DEFAULT_MAX_RETRY_TIMES;
            this.username = DEFAULT_USERNAME;
            this.password = DEFAULT_PASSWORD;
            this.enableHighAvailable = DEFAULT_ENABLE_HIGH_AVAILABLE;
            this.autoLoadAvailableList = DEFAULT_AUTO_LOAD_AVAILABLE_LIST;
            this.maxSwitchTimes = DEFAULT_MAX_SWITCH_TIMES;
            this.loadAvailableListInterval = DEFAULT_LOAD_AVAILABLE_LIST_INTERVAL;
            this.loadAvailableListIntervalUnit = DEFAULT_LOAD_AVAILABLE_LIST_INTERVAL_Unit;
            this.socketTimeout = DEFAULT_SOCKET_TIMEOUT;
            this.connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
        }

        public Builder timeout(int timeout) {
            this.socketTimeout = timeout;
            this.connectionTimeout = timeout;
            return this;
        }

        public Builder socketTimeout(int timeout) {
            this.socketTimeout = timeout;
            return this;
        }

        public Builder connectionTimeout(int timeout) {
            this.connectionTimeout = timeout;
            return this;
        }

        public Builder connectionString(String connectionString) {
            this.connectionString = connectionString;
            return this;
        }

        public Builder enableRetry() {
            this.enableRetry = true;
            return this;
        }

        public Builder enableRetry(int maxRetryTimes) {
            this.enableRetry = true;
            this.maxRetryTimes = maxRetryTimes;
            return this;
        }

        public Builder disableRetry() {
            this.enableRetry = false;
            this.maxRetryTimes = DEFAULT_MAX_RETRY_TIMES;
            return this;
        }

        public Builder auth(String username, String password) {
            this.username = username;
            this.password = password;
            return this;
        }

        public Builder enableHighAvailable() {
            this.enableHighAvailable = true;
            return this;
        }

        public Builder enableHighAvailable(int maxSwitchTimes) {
            this.enableHighAvailable = true;
            this.maxSwitchTimes = maxSwitchTimes;
            return this;
        }

        public Builder disableHighAvailable() {
            this.enableHighAvailable = false;
            this.maxSwitchTimes = DEFAULT_MAX_SWITCH_TIMES;
            return this;
        }

        public Builder enableAutoLoadAvailableList() {
            this.autoLoadAvailableList = true;
            return this;
        }

        public Builder enableAutoLoadAvailableList(long interval, TimeUnit Unit) {
            this.autoLoadAvailableList = true;
            this.loadAvailableListInterval = interval;
            this.loadAvailableListIntervalUnit = Unit;
            return this;
        }

        public Builder disableAutoLoadAvailableList() {
            this.autoLoadAvailableList = false;
            this.loadAvailableListInterval = DEFAULT_LOAD_AVAILABLE_LIST_INTERVAL;
            this.loadAvailableListIntervalUnit = DEFAULT_LOAD_AVAILABLE_LIST_INTERVAL_Unit;
            return this;
        }

        public IGinXSessionSettings build() {
            if (this.connectionString == null) {
                throw new IllegalArgumentException("connectionString is null");
            }
            String[] endPointStrings = this.connectionString.split(",");
            IGinXEndPoint[] endPoints = new IGinXEndPoint[endPointStrings.length];
            for (int i = 0; i < endPointStrings.length; i++) {
                String[] parts = endPointStrings[i].split(":");
                String ip = parts[0];
                int port = Integer.parseInt(parts[1]);
                endPoints[i] = new IGinXEndPoint(ip, port);
            }
            return new IGinXSessionSettings(endPoints, enableRetry, maxRetryTimes, username, password, enableHighAvailable, autoLoadAvailableList, maxSwitchTimes, loadAvailableListInterval, loadAvailableListIntervalUnit, socketTimeout, connectionTimeout);
        }

    }

}
