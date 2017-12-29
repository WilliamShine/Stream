package com.YunTu.TopoLine.SelectDriver.tool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.storm.jdbc.common.ConnectionProvider;



/**
 * Mysql 连接池
 * storm集成jdbc的 连接提供者
 * 连接提供器，用于获取mysql数据库连接
 * @author Administrator
 *
 */

//为jdbcBolt组件提供对应的数据连接
public class MysqlConnectionProvider implements ConnectionProvider {

    private static String driver = "com.mysql.jdbc.driver";
    private static String url = "jdbc:mysql://47.100.9.7:3306/data_warehouse";
    private static String user = "weiwei.wu";
    private static String password = "Miweiwei20170711@";
    
    static{
        
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            new ExceptionInInitializerError(e);
//            e.printStackTrace();
        }
        
    }
    
    public void cleanup() {
    	//TODO

    }

    public Connection getConnection() {
        try {
            return DriverManager.getConnection(url,user,password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void prepare() {
    	//TODO 

    }
    
   /* @Test
    public void jdbcTest() {
		Connection con = new MysqlConnectionProvider().getConnection();
		try {
			Statement stat = con.createStatement();
			ResultSet result = stat.executeQuery("select * from dw_ml_users");
			while (result.next()) {
				int id = result.getInt(1);
				String name = result.getString(2);
				System.out.println(id + "--" + name);
			}
			if (stat != null)
				stat.close();
			if (con != null)
				con.close();
			System.out.println("------over-----------");
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}*/

}