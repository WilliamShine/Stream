package com.YunTu.VCount;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.junit.Test;

import com.YunTu.uvCount.C.MysqlConnectionProvider;

public class SqlTest {

	@Test
	public void XiuGai3(){
        Connection conn=null;
        PreparedStatement ps=null;
        String sql="update sy_count set UV=? where id=?";//update sy_count set UV=?,PV=? where id=?
        conn=new MysqlConnectionProvider().getConnection();
        try {
            ps=conn.prepareStatement(sql);
            ps.setInt(1,1111111 );
            ps.setInt(2,20171219 );
            int i= ps.executeUpdate();
            System.out.println(i);
            
        } catch (SQLException e) {
            e.printStackTrace();
        }finally{
            //DBConnUtil.closeAll(null, ps, conn);
        }
//        return flag;
    }
}
