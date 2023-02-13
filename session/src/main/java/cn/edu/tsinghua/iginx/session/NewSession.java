package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.thrift.IService;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class NewSession {

    private static final Logger logger = LoggerFactory.getLogger(NewSession.class);

    private IService.Iface client;

    private TTransport transport;

    private static class IGinXEndPoints {

        private String[] ips;

        private int[] ports;

        public IGinXEndPoints(String ip, int port) {
            ips = new String[] {ip};
            ports = new int[] {port};
        }

        public IGinXEndPoints(String connectionString) {

        }

    }

}
