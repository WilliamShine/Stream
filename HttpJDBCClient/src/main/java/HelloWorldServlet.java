import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class HelloWorldServlet extends HttpServlet {  
    private static final long serialVersionUID = 1L;  
  
  
    @Override  
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)  
        throws ServletException, IOException {  
        //如果要想显示中文，则即必须在获取流之前，先添加如下的代码，设置好解析用的字符集  
        resp.setContentType("text/html;charset=gbk");  
  
  
        PrintWriter out = resp.getWriter();  
  
  
        //这个字符串里的内容就是在浏览器上显示的内容  
        String str = "<html><title></title><body>你好Hello World Servlet!!!</body></html>";  
        //将流中的内容写出去，  
        out.println(str);  
        out.flush();  
        out.close();  
        System.out.println("doget...");  
    }  
  
  
    @Override  
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)  
        throws ServletException, IOException {  
        PrintWriter out = resp.getWriter();  
        String str = "<html><title></title><body>你好Hello World Servlet!!!</body></html>";  
        out.println(str);  
        out.flush();  
        out.close();  
        System.out.println("dopost...");  
    }  
}  