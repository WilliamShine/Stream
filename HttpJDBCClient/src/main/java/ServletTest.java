
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.Test;

/** 
 * 发送给客户端 
 *  
 * @author hongjie 
 *  
 */ 
public class ServletTest extends HttpServlet {

	protected Connection con;  
    protected Statement ps;  
    protected ResultSet rs;  
  
    public void doGet(HttpServletRequest request, HttpServletResponse response)  
            throws ServletException, IOException {  
        response.setContentType("text/html;charset=gb2312");  
        PrintWriter out = response.getWriter();  
        // 连接Mysql  
        Tabletest test = new Tabletest();  
        // 打印获取的查询数据  
        out.println(test.GetAllName().toString());  
  
    } 
}
