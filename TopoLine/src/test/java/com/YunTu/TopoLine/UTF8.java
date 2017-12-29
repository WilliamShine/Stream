package com.YunTu.TopoLine;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.YunTu.TopoLine.SelectDriver.tool.MysqlConnectionProvider;

public class UTF8 {

	/*@Test
	public void test1() {
		StringBuffer sb = new StringBuffer();  
	    sb.append("小米");  
	    String xmString = "";  
	    String xmlUTF8="";  
	    try {  
	    xmString = new String(sb.toString().getBytes("UTF-8"));  
	    xmlUTF8 = URLEncoder.encode(xmString, "UTF-8");
	    System.out.println("xmString" + xmString) ;  
	    System.out.println("utf-8 编码：" + xmlUTF8) ;  
	    } catch (UnsupportedEncodingException e) {  
	    e.printStackTrace();  
	    }  
	}
	
	@Test
	public void test2() {
		Connection connection = new MysqlConnectionProvider().getConnection();
		try {
			String qure1 = "insert into DTstorm_result(dwtvs_task,dwtvs_logo,dwtvs_name,dwtvs_date_year,dwtvs_date_month,dwtvs_date_day,dwtvs_date_hour,dwtvs_count_num,dwtvs_type) values (?,?,?,?,?,?,?,?,?)";
			String qure2 = "insert into shine_test(id,name,num) values (?,?,?)";
			PreparedStatement preparedStatement = connection.prepareStatement(qure1);
			System.out.println(preparedStatement);
			preparedStatement.setString(2, "小米");
			System.out.println(preparedStatement);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	*/
	
	@Test
	public void test2() {
		List<String> list1 =new ArrayList<String>();
		list1.add("微鲸");
		System.out.println(list1.contains("风行"));
		String year = null;
		System.out.println(year==null);
	}
}
