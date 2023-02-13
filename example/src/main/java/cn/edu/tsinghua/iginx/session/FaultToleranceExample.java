package cn.edu.tsinghua.iginx.session;

import java.util.Arrays;

public class FaultToleranceExample {

    public static void main(String[] args) throws Exception {
        Session session = new Session("127.0.0.1", 6888, "root", "root");
        session.openSession();

        StringBuilder builder2 = new StringBuilder();
        QueryDataSet dataSet2 = session.executeQuery(7036197063314452480L, "select a.curr_load as load, a.truck as truck, b.capacity as capacity from (select truck, last_value(value) as curr_load from (select transposition(*) from diagnostics.*.East.*.*.*) group by name, truck having name = 'current_load') as a, (select truck, last_value(value) as capacity from (select transposition(*) from diagnostics.*.East.*.*.*) group by name, truck having name = 'load_capacity') as b where a.truck = b.truck", 3);
            for (String column: dataSet2.getColumnList()) {
                builder2.append(column).append(" ");
            }
            while (dataSet2.hasMore()) {
                builder2.append(Arrays.toString(dataSet2.nextRow()));
                builder2.append('\t');
            }
            dataSet2.close();
        System.out.println(builder2);


//        int cnt = 0;
//        for (int i = 0; i < 100; i++) {
//            StringBuilder builder1 = new StringBuilder();
//            StringBuilder builder2 = new StringBuilder();
//
//            QueryDataSet dataSet1 = session.executeQuery("select * from m order by key desc", 3);
//            for (String column: dataSet1.getColumnList()) {
//                builder1.append(column).append(" ");
//            }
//            while (dataSet1.hasMore()) {
//                builder1.append(Arrays.toString(dataSet1.nextRow()));
//                builder1.append('\t');
//            }
//            dataSet1.close();
//
//            QueryDataSet dataSet2 = session.executeQuery(dataSet1.getQueryId(), "select * from m order by key desc", 3);
//            for (String column: dataSet2.getColumnList()) {
//                builder2.append(column).append(" ");
//            }
//            while (dataSet2.hasMore()) {
//                builder2.append(Arrays.toString(dataSet2.nextRow()));
//                builder2.append('\t');
//            }
//            dataSet2.close();
//
//            if (!builder1.toString().equals(builder2.toString())) {
//                System.out.println("unexpected unequal:");
//                System.out.println("str1: " + builder1);
//                System.out.println("str2: " + builder2);
//                System.out.println("queryId: " + dataSet1.getQueryId());
//                System.out.println();
//                break;
//            }
//        }
//        System.out.println(cnt);
//
//        session.closeSession();

    }

}
