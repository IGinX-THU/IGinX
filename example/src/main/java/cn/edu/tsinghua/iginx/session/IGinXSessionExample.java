package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.fault_tolerance.IGinXSession;
import cn.edu.tsinghua.iginx.fault_tolerance.IGinXSessionSettings;

public class IGinXSessionExample {

    public static void main(String[] args) throws Exception {
        IGinXSession session = new IGinXSession(new IGinXSessionSettings
                .Builder()
                .connectionString("127.0.0.1:6888,127.0.0.1:6880")
                .build()
        );
        session.open();
        System.out.println("replica num: " + session.getReplicaNum());
        session.close();
    }

}
