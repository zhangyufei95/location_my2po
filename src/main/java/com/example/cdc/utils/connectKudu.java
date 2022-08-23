package com.example.cdc.utils;

import java.sql.*;

public class connectKudu {
    static String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    static String CONNECTION_URL = "jdbc:impala://172.27.16.27:27009/stage;UseSasl=0;AuthMech=3;UID=hadoop;PWD=hadoop";

    public static void main(String[] args) {
        Connection con = null;
        ResultSet rs = null;
        PreparedStatement ps = null;

        try {
            Class.forName(JDBC_DRIVER);
            con = DriverManager.getConnection(CONNECTION_URL);

            String sql2 = "select * from stage.stage_call_driver order by create_time desc";
            ps = con.prepareStatement(sql2);
            rs = ps.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getString(1) + "\t" + rs.getString(2));
            }
//            ps.setInt(1, 81);
//            ps.setString(2, "测试中文字符");
//            ps.execute();
//            ps.close();

//            ps = con.prepareStatement("select * from my_first_table order byid asc");
//            rs = ps.executeQuery();
//            while (rs.next()) {
//                System.out.println(rs.getLong(1) + "\t" + rs.getString(2));
//            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {// 关闭rs、ps和con
                rs.close();
                ps.close();
                con.close();
            } catch (SQLException e) {
                // TODOAuto-generated catch block
                e.printStackTrace();
            }

        }
    }
}