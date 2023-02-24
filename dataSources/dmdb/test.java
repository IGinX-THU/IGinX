/*该例程实现插入数据，修改数据，删除数据，数据查询等基本操作。*/
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.font.FontRenderContext;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import javax.imageio.ImageIO;
public class BasicApp {
    // 定义DM JDBC驱动串
    String jdbcString = "dm.jdbc.driver.DmDriver";
    // 定义DM URL连接串
    String urlString = "jdbc:dm://localhost:5236";
    // 定义连接用户名
    String userName = "SYSDBA";
    // 定义连接用户口令
    String password = "SYSDBA001";
    // 定义连接对象
    Connection conn = null;
    /* 加载JDBC驱动程序
     * @throws SQLException 异常 */
    public void loadJdbcDriver() throws SQLException {
        try {
            System.out.println("Loading JDBC Driver...");
            // 加载JDBC驱动程序
            Class.forName(jdbcString);
        } catch (ClassNotFoundException e) {
            throw new SQLException("Load JDBC Driver Error : " + e.getMessage());
        } catch (Exception ex) {
            throw new SQLException("Load JDBC Driver Error : "
                    + ex.getMessage());
        }
    }
    /* 连接DM数据库
     * @throws SQLException 异常 */
    public void connect() throws SQLException {
        try {
            System.out.println("Connecting to DM Server...");
            // 连接DM数据库
            conn = DriverManager.getConnection(urlString, userName, password);
        } catch (SQLException e) {
            throw new SQLException("Connect to DM Server Error : "
                    + e.getMessage());
        }
    }
    /* 关闭连接
     * @throws SQLException 异常 */
    public void disConnect() throws SQLException {
        try {
            // 关闭连接
            conn.close();
        } catch (SQLException e) {
            throw new SQLException("close connection error : " + e.getMessage());
        }
    }
    /* 往产品信息表插入数据
     * @throws SQLException 异常 */
    public void insertTable() throws SQLException {
        // 插入数据语句
        String sql = "INSERT INTO production.product(name,author,publisher,publishtime,"
                + "product_subcategoryid,productno,satetystocklevel,originalprice,nowprice,discount,"
                + "description,photo,type,papertotal,wordtotal,sellstarttime,sellendtime) "
                + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
        // 创建语句对象
        PreparedStatement pstmt = conn.prepareStatement(sql);
        // 为参数赋值
        pstmt.setString(1, "三国演义");
        pstmt.setString(2, "罗贯中");
        pstmt.setString(3, "中华书局");
        pstmt.setDate(4, Date.valueOf("2005-04-01"));
        pstmt.setInt(5, 4);
        pstmt.setString(6, "9787101046121");
        pstmt.setInt(7, 10);
        pstmt.setBigDecimal(8, new BigDecimal(19.0000));
        pstmt.setBigDecimal(9, new BigDecimal(15.2000));
        pstmt.setBigDecimal(10, new BigDecimal(8.0));
        pstmt.setString(11, "《三国演义》是中国第一部长篇章回体小说，中国小说由短篇发展至长篇的原因与说书有关。");
        // 设置大字段参数
        try {
            // 创建一个图片用于插入大字段
            String filePath = "c:\\三国演义.jpg";
            CreateImage(filePath);
            File file = new File(filePath);
            InputStream in = new BufferedInputStream(new FileInputStream(file));
            pstmt.setBinaryStream(12, in, (int) file.length());
        } catch (FileNotFoundException e) {
            System.out.println(e.getMessage());
            // 如果没有图片设置为NULL
            pstmt.setNull(12, java.sql.Types.BINARY);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        pstmt.setString(13, "25");
        pstmt.setInt(14, 943);
        pstmt.setInt(15, 93000);
        pstmt.setDate(16, Date.valueOf("2006-03-20"));
        pstmt.setDate(17, Date.valueOf("1900-01-01"));
        // 执行语句
        pstmt.executeUpdate();
        // 关闭语句
        pstmt.close();
    }
    /* 查询产品信息表
     * @throws SQLException 异常 */
    public void queryProduct() throws SQLException {
        // 查询语句
        String sql = "SELECT productid,name,author,description,photo FROM production.product WHERE productid=11";
        // 创建语句对象
        Statement stmt = conn.createStatement();
        // 执行查询
        ResultSet rs = stmt.executeQuery(sql);
        // 显示结果集
        displayResultSet(rs);
        // 关闭结果集
        rs.close();
        // 关闭语句
        stmt.close();
    }
    /* 修改产品信息表数据
     * @throws SQLException 异常 */
    public void updateTable() throws SQLException {
        // 更新数据语句
        String sql = "UPDATE production.product SET name = ?"
                + "WHERE productid = 11;";
        // 创建语句对象
        PreparedStatement pstmt = conn.prepareStatement(sql);
        // 为参数赋值
        pstmt.setString(1, "三国演义（上）");
        // 执行语句
        pstmt.executeUpdate();
        // 关闭语句
        pstmt.close();
    }
    /* 删除产品信息表数据
     * @throws SQLException 异常 */
    public void deleteTable() throws SQLException {
        // 删除数据语句
        String sql = "DELETE FROM production.product WHERE productid = 11;";
        // 创建语句对象
        Statement stmt = conn.createStatement();
        // 执行语句
        stmt.executeUpdate(sql);
        // 关闭语句
        stmt.close();
    }
    /* 查询产品信息表
     * @throws SQLException 异常 */
    public void queryTable() throws SQLException {
        // 查询语句
        String sql = "SELECT productid,name,author,publisher FROM production.product";
        // 创建语句对象
        Statement stmt = conn.createStatement();
        // 执行查询
        ResultSet rs = stmt.executeQuery(sql);
        // 显示结果集
        displayResultSet(rs);
        // 关闭结果集
        rs.close();
        // 关闭语句
        stmt.close();
    }
    /* 调用存储过程修改产品信息表数据
     * @throws SQLException 异常 */
    public void updateProduct() throws SQLException {
        // 更新数据语句
        String sql = "{ CALL production.updateProduct(?,?) }";
        // 创建语句对象
        CallableStatement cstmt = conn.prepareCall(sql);
        // 为参数赋值
        cstmt.setInt(1, 1);
        cstmt.setString(2, "红楼梦（上）");
        // 执行语句
        cstmt.execute();
        // 关闭语句
        cstmt.close();
    }
    /* 显示结果集
     * @param rs 结果集对象
     * @throws SQLException 异常 */
    private void displayResultSet(ResultSet rs) throws SQLException {
        // 取得结果集元数据
        ResultSetMetaData rsmd = rs.getMetaData();
        // 取得结果集所包含的列数
        int numCols = rsmd.getColumnCount();
        // 显示列标头
        for (int i = 1; i <= numCols; i++) {
            if (i > 1) {
                System.out.print(",");
            }
            System.out.print(rsmd.getColumnLabel(i));
        }
        System.out.println("");
        // 显示结果集中所有数据
        while (rs.next()) {
            for (int i = 1; i <= numCols; i++) {
                if (i > 1) {
                    System.out.print(",");
                }
                // 处理大字段
                if ("IMAGE".equals(rsmd.getColumnTypeName(i))) {
                    byte[] data = rs.getBytes(i);
                    if (data != null && data.length > 0) {
                        FileOutputStream fos;
                        try {
                            fos = new FileOutputStream("c:\\三国演义1.jpg");
                            fos.write(data);
                            fos.close();
                        } catch (FileNotFoundException e) {
                            System.out.println(e.getMessage());
                        } catch (IOException e) {
                            System.out.println(e.getMessage());
                        }
                    }
                    System.out.print("字段内容已写入文件c:\\三国演义1.jpg，长度" + data.length);
                } else {
                    // 普通字段
                    System.out.print(rs.getString(i));
                }
            }
            System.out.println("");
        }
    }
    /* 创建一个图片用于插入大字段
     * @throws IOException 异常 */
    private void CreateImage(String path) throws IOException {
        int width = 100;
        int height = 100;
        String s = "三国演义";
        File file = new File(path);
        Font font = new Font("Serif", Font.BOLD, 10);
        BufferedImage bi = new BufferedImage(width, height,
                BufferedImage.TYPE_INT_RGB);
        Graphics2D g2 = (Graphics2D) bi.getGraphics();
        g2.setBackground(Color.WHITE);
        g2.clearRect(0, 0, width, height);
        g2.setPaint(Color.RED);
        FontRenderContext context = g2.getFontRenderContext();
        Rectangle2D bounds = font.getStringBounds(s, context);
        double x = (width - bounds.getWidth()) / 2;
        double y = (height - bounds.getHeight()) / 2;
        double ascent = -bounds.getY();
        double baseY = y + ascent;
        g2.drawString(s, (int) x, (int) baseY);
        ImageIO.write(bi, "jpg", file);
    }

    //类主方法 @param args 参数
    public static void main(String args[]) {
        try {
            // 定义类对象
            BasicApp basicApp = new BasicApp();
            // 加载驱动程序
            basicApp.loadJdbcDriver();
            // 连接DM数据库
            basicApp.connect();
            // 插入数据
            System.out.println("--- 插入产品信息 ---");
            basicApp.insertTable();
            // 查询含有大字段的产品信息
            System.out.println("--- 显示插入结果 ---");
            basicApp.queryProduct();
            // 在修改前查询产品信息表
            System.out.println("--- 在修改前查询产品信息 ---");
            basicApp.queryTable();
            // 修改产品信息表
            System.out.println("--- 修改产品信息 ---");
            basicApp.updateTable();
            // 在修改后查询产品信息表
            System.out.println("--- 在修改后查询产品信息 ---");
            basicApp.queryTable();
            // 删除产品信息表
            System.out.println("--- 删除产品信息 ---");
            basicApp.deleteTable();
            // 在删除后查询产品信息表
            System.out.println("--- 在删除后查询产品信息 ---");
            basicApp.queryTable();
            // 调用存储过程修改产品信息表
            System.out.println("--- 调用存储过程修改产品信息 ---");
            basicApp.updateProduct();
            // 在存储过程更新后查询产品信息表
            System.out.println("--- 调用存储过程后查询产品信息 ---");
            basicApp.queryTable();
            // 关闭连接
            basicApp.disConnect();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
}
