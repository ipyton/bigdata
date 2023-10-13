package hive;

import java.sql.*;

public class HiveJDBC {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        Connection conn = DriverManager.getConnection("jdbc:hive2://node04:10000/default", "root", "");
        Statement stmt = conn.createStatement();
        String sql = "select * from psn limit 5";
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "-" + res.getString("name"));
        }
    }
}
