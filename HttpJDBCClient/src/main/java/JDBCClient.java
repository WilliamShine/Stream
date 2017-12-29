
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/** 
 * 连接数据库 
 *  
 * @author hongjie 
 *  
 */  
public class JDBCClient {

	/** 
	 * 连接数据库 
	 *  
	 * @author hongjie 
	 *  
	 */  
	    protected Connection con;  
	    protected PreparedStatement ps;  
	    protected ResultSet res;  
	    /** url指定访问的数据库 */  
	    private String url;  
	    /** 数据库的用户名 */  
	    private String user;  
	    /** 数据库的密码 */  
	    private String passwd;  
	  
	    /** 
	     * 构造器，初始化数据 
	     */  
	    public JDBCClient() {  
	        this.url = "jdbc:mysql://106.14.248.228:23306/data_warehouse";  
	        this.user = "weiwei.wu";  
	        this.passwd = "Miweiwei20170711@";  
	    }  
	  
	    /** 
	     * 连接数据库 
	     */  
	    public void BuildConnection() {  
	        try {  
	            // 加载驱动  
	        	Class.forName("com.mysql.jdbc.Driver");
	            // 连接数据库  
	            con = DriverManager.getConnection(url, user, passwd);  
	        } catch (Exception e) {  
	            e.printStackTrace();  
	        }  
	    }  
	  
	    /** 
	     * 关闭开启的相关操作 
	     */  
	    public void close() {  
	        try {  
	            if (res != null)  
	                res.close();  
	            if (ps != null)  
	                ps.close();  
	            if (con != null)  
	                con.close();  
	        } catch (Exception e) {  
	            e.printStackTrace();  
	        }  
	    }  
	  
}
