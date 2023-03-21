package cn.edu.tsinghua.iginx.integration.expansion.postgresql;

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgreSQLHistoryDataGeneratorTest extends BaseHistoryDataGenerator {

    private static final Logger logger = LoggerFactory.getLogger(PostgreSQLHistoryDataGeneratorTest.class);

    private static final String CREATE_DATABASE_STATEMENT = "CREATE DATABASE %s;";

    private static final String CREATE_TABLE_STATEMENT = "CREATE TABLE %s (time BIGINT NOT NULL, %s, PRIMARY KEY(time));";

    private static final String INSERT_STATEMENT = "INSERT INTO %s VALUES %s;";

    private static final String DROP_DATABASE_STATEMENT = "DROP DATABASE %s;";

    private static final int PORT_A = 5432;

    private static final int PORT_B = 5433;

    private static final String USERNAME = "postgres";

    private static final String PASSWORD = "postgres";

    private static final String DATABASE_NAME = "ln";

    private Connection connect(int port) {
        try {
            String url = String.format("jdbc:postgresql://127.0.0.1:%d/", port);
            Class.forName("org.postgresql.Driver");
            return DriverManager.getConnection(url, USERNAME, PASSWORD);
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void clearData() {
        try {
            Connection connA = connect(PORT_A);
            Statement stmtA = connA.createStatement();
            stmtA.execute(String.format(DROP_DATABASE_STATEMENT, DATABASE_NAME));
            stmtA.close();
            connA.close();

            Connection connB = connect(PORT_B);
            Statement stmtB = connB.createStatement();
            stmtB.execute(String.format(DROP_DATABASE_STATEMENT, DATABASE_NAME));
            stmtB.close();
            stmtB.close();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        logger.info("clear data success!");
    }

    @Test
    public void writeHistoryDataToA() throws Exception {
        Connection conn = connect(PORT_A);
        if (conn == null) {
            logger.error("cannot connect to 5432!");
            return;
        }

        Statement stmt = conn.createStatement();
        try {
            stmt.execute(String.format(CREATE_DATABASE_STATEMENT, DATABASE_NAME));
        } catch (SQLException e) {
            logger.info("database ln exists!");
        }

        stmt.execute(String.format(CREATE_TABLE_STATEMENT, "wf01", "status boolean, temperature float8"));

        stmt.execute(String.format(INSERT_STATEMENT, "wf01", "(100, true, null), (200, false, 20.71)"));

        stmt.close();
        conn.close();
        logger.info("write data to 127.0.0.1:5432 success!");
    }

    @Test
    public void writeHistoryDataToB() throws Exception {
        Connection conn = connect(PORT_B);
        if (conn == null) {
            logger.error("cannot connect to 5433!");
            return;
        }

        Statement stmt = conn.createStatement();
        try {
            stmt.execute(String.format(CREATE_DATABASE_STATEMENT, DATABASE_NAME));
        } catch (SQLException e) {
            logger.info("database ln exists!");
        }

        stmt.execute(String.format(CREATE_TABLE_STATEMENT, "wf03", "status boolean, temperature float8"));

        stmt.execute(String.format(INSERT_STATEMENT, "wf03", "(77, true, null), (200, false, 77.71)"));

        logger.info("write data to 127.0.0.1:5433 success!");
    }
}
