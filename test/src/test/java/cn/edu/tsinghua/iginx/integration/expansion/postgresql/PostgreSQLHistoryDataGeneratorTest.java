package cn.edu.tsinghua.iginx.integration.expansion.postgresql;

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgreSQLHistoryDataGeneratorTest extends BaseHistoryDataGenerator {

    private static final Logger logger =
            LoggerFactory.getLogger(PostgreSQLHistoryDataGeneratorTest.class);

    public static final char IGINX_SEPARATOR = '.';

    public static final char POSTGRESQL_SEPARATOR = '\u2E82';

    private static final String CREATE_DATABASE_STATEMENT = "CREATE DATABASE %s;";

    private static final String CREATE_TABLE_STATEMENT = "CREATE TABLE %s (%s);";

    private static final String INSERT_STATEMENT = "INSERT INTO %s VALUES %s;";

    private static final String DROP_DATABASE_STATEMENT = "DROP DATABASE %s;";

    private static final String USERNAME = "postgres";

    private static final String PASSWORD = "postgres";

    private static final Set<String> databaseNameList = new HashSet<>();

    public PostgreSQLHistoryDataGeneratorTest() {
        this.portOri = 6667;
        this.portExp = 6668;
    }

    private Connection connect(int port, boolean useSystemDatabase, String databaseName) {
        try {
            String url;
            if (useSystemDatabase) {
                url = String.format("jdbc:postgresql://127.0.0.1:%d/", port);
            } else {
                url = String.format("jdbc:postgresql://127.0.0.1:%d/%s", port, databaseName);
            }
            Class.forName("org.postgresql.Driver");
            return DriverManager.getConnection(url, USERNAME, PASSWORD);
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeHistoryDataToOri() {
        writeHistoryData(PATH_LIST_ORI, DATA_TYPE_LIST_ORI, VALUES_LIST_ORI, portOri);
    }

    @Override
    public void writeHistoryDataToExp() {
        writeHistoryData(PATH_LIST_EXP, DATA_TYPE_LIST_EXP, VALUES_LIST_EXP, portExp);
    }

    private void writeHistoryData(
            List<String> pathList,
            List<DataType> dataTypeList,
            List<List<Object>> valuesList,
            int port) {
        try {
            Connection conn = connect(port, true, null);
            if (conn == null) {
                logger.error("cannot connect to 127.0.0.1:{}!", port);
                return;
            }

            Map<String, Map<String, List<Integer>>> databaseToTablesToColumnIndexes =
                    new HashMap<>();
            for (int i = 0; i < pathList.size(); i++) {
                String path = pathList.get(i);
                String databaseName = path.substring(0, path.indexOf(IGINX_SEPARATOR));
                String tableName =
                        path.substring(
                                        path.indexOf(IGINX_SEPARATOR) + 1,
                                        path.lastIndexOf(IGINX_SEPARATOR))
                                .replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);

                Map<String, List<Integer>> tablesToColumnIndexes =
                        databaseToTablesToColumnIndexes.computeIfAbsent(
                                databaseName, x -> new HashMap<>());
                List<Integer> columnIndexes =
                        tablesToColumnIndexes.computeIfAbsent(tableName, x -> new ArrayList<>());
                columnIndexes.add(i);
                tablesToColumnIndexes.put(tableName, columnIndexes);
            }

            for (Map.Entry<String, Map<String, List<Integer>>> entry :
                    databaseToTablesToColumnIndexes.entrySet()) {
                String databaseName = entry.getKey();
                Statement stmt = conn.createStatement();
                try {
                    stmt.execute(String.format(CREATE_DATABASE_STATEMENT, databaseName));
                    databaseNameList.add(databaseName);
                } catch (SQLException e) {
                    logger.info("database {} exists!", databaseName);
                }
                stmt.close();
                conn.close();

                conn = connect(port, false, databaseName);
                stmt = conn.createStatement();
                for (Map.Entry<String, List<Integer>> item : entry.getValue().entrySet()) {
                    String tableName = item.getKey();
                    StringBuilder createTableStr = new StringBuilder();
                    for (Integer index : item.getValue()) {
                        String columnName = pathList.get(index);
                        DataType dataType = dataTypeList.get(index);
                        createTableStr.append(columnName);
                        createTableStr.append(" ");
                        createTableStr.append(toPostgreSQL(dataType));
                        createTableStr.append(", ");
                    }
                    stmt.execute(
                            String.format(
                                    CREATE_TABLE_STATEMENT,
                                    tableName,
                                    createTableStr.substring(0, createTableStr.length() - 2)));

                    StringBuilder insertStr = new StringBuilder();
                    for (List<Object> values : valuesList) {
                        insertStr.append("(");
                        for (Integer index : item.getValue()) {
                            insertStr.append(values.get(index));
                            insertStr.append(", ");
                        }
                        insertStr =
                                new StringBuilder(insertStr.substring(0, insertStr.length() - 2));
                        insertStr.append("), ");
                    }
                    stmt.execute(
                            String.format(
                                    INSERT_STATEMENT,
                                    tableName,
                                    insertStr.substring(0, insertStr.length() - 2)));
                }
                stmt.close();
                conn.close();
            }

            logger.info("write data to 127.0.0.1:{} success!", port);
        } catch (RuntimeException | SQLException e) {
            logger.error("write data to 127.0.0.1:{} failure: {}", port, e.getMessage());
        }
    }

    @Test
    @Override
    public void clearHistoryData() {
        clearHistoryData(portOri);
        clearHistoryData(portExp);
    }

    private void clearHistoryData(int port) {
        try {
            Connection conn = connect(port, true, null);
            Statement stmt = conn.createStatement();
            for (String databaseName : databaseNameList) {
                stmt.execute(String.format(DROP_DATABASE_STATEMENT, databaseName));
            }
            stmt.close();
            conn.close();
            logger.info("clear data on 127.0.0.1:{} success!", port);
        } catch (SQLException e) {
            logger.error("clear data on 127.0.0.1:{} failure: {}", port, e.getMessage());
        }
    }

    private static String toPostgreSQL(DataType dataType) {
        switch (dataType) {
            case BOOLEAN:
                return "BOOLEAN";
            case INTEGER:
                return "INTEGER";
            case LONG:
                return "BIGINT";
            case FLOAT:
                return "REAL";
            case DOUBLE:
                return "DOUBLE PRECISION";
            case BINARY:
            default:
                return "TEXT";
        }
    }
}
