
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import net.sf.json.JSONArray;

/** 
 * test 表操作 
 *  
 * @author hongjie 
 *  
 */  
public class Tabletest extends JDBCClient{

	private List list;  
    private ResultSet res;  
    private String sql;  
  
    public String GetAllName() {  
        // 建立连接  
        this.BuildConnection();  
  
        String str="";  
        try {  
            sql = "select * from sy_count";  
            this.ps = con.prepareStatement(sql);  
            res = ps.executeQuery(); 
            
            List<Map<String, Object>> jsonlist = new ArrayList<>();
            Map<String, Object> map =null;
            while (res.next()) {  
            	map=new HashMap<>();
            	map.put("id",res.getString("id"));
            	map.put("UV", res.getString("UV"));
            	map.put("PV", res.getString("PV"));
            	jsonlist.add(map);
            }  
            JSONArray json = JSONArray.fromObject(jsonlist);
            str =json.toString();
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
        this.close();  
        return str;  
    }  
    
    @Test
    public void test() {
		System.out.println(GetAllName());
	}
  
}
