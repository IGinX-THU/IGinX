package cn.edu.tsinghua.iginx.fault_tolerance;

public class IGinXEndPoint {

    private final String ip;

    private final int port;

    public IGinXEndPoint(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "{" +
                "ip='" + ip + '\'' +
                ", port=" + port +
                '}';
    }
}
