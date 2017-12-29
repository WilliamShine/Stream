package com.YunTu.TopoLine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.YunTu.TopoLine.SelectDriver.OriginalTopology;

import net.sf.json.JSONObject;

public class JosnTest {
	@Test
	public void Test1() {
		
		try {
			JosnTest.class.getClassLoader().getResourceAsStream("josn");//类加载器流在这不适用于读取外部配置
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(JosnTest.class.getClassLoader().getResourceAsStream("josn")));
			String line = null;
			while (bufferedReader.ready()) {
				line=bufferedReader.readLine();
			}
			JSONObject json = JSONObject.fromObject(line);
			/*Object obj = json.get("DC001_/eb/");
			Map<String, Object> map = (Map<String, Object>) obj;
			Set set=map.keySet();
			for (Object object : set) {
				System.out.println(object+"	"+map.get(object));
			}*/
	    	 
	    	 Map<String, Object> jsonMap = (Map<String, Object>) json.get("DC001_/eb/");
	    	 List<String> Limitset=(List<String>) jsonMap.get("限制条件");
	    	 Map<String, List<String>> DimensionMap=(Map<String, List<String>>) jsonMap.get("维度");
	    	 
	    	 System.out.println(DimensionMap);
			bufferedReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
