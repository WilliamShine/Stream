package com.YunTu.BigTable.flyTable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;


public class FlyCount {

	public static void main(String[] args) throws Exception {
		String beiJing = "116.405285,39.904989";
		String shanghai="121.472644,31.231706";
		String guangzhou="113.280637,23.125178";
		String shenzhen="114.085947,22.547";
		String hangzhou="120.153576,30.287459";
		String chengdu="104.065735,30.659462";
		String haerbing="126.642464,45.756967";
		String wulumuqi="87.617733,43.792818";

		ArrayList<Map<String, Object>> list = new ArrayList<>();
		BufferedReader bufferedReader = new BufferedReader(new FileReader("src/main/java/flydata.csv"));
		Map<String, Integer> map = new HashMap<>();
		while (bufferedReader.ready()) {
			String line =bufferedReader.readLine();
			String toGo = line.split(",", -1)[7];
			Integer num = map.get(toGo);
			if (num!=null) {
				map.put(toGo, ++num);
			} else {
				map.put(toGo, 1);
			}
		}
		bufferedReader.close();
		Set<String> set = map.keySet();
		Map<String, Object> nummap =null;
		for (String string : set) {
			switch (string) {
			case "上海":
				nummap = new HashMap<>();
				nummap.put("lat", Double.parseDouble(shanghai.split(",",-1)[1]));
				nummap.put("lng", Double.parseDouble(shanghai.split(",",-1)[0]));
				nummap.put("value", map.get(string));
				list.add(nummap);
				break;
			case "广州":
				nummap = new HashMap<>();
				nummap.put("lat", Double.parseDouble(guangzhou.split(",",-1)[1]));
				nummap.put("lng", Double.parseDouble(guangzhou.split(",",-1)[0]));
				nummap.put("value", map.get(string));
				list.add(nummap);
				break;
			case "哈尔滨":
				nummap = new HashMap<>();
				nummap.put("lat", Double.parseDouble(haerbing.split(",",-1)[1]));
				nummap.put("lng", Double.parseDouble(haerbing.split(",",-1)[0]));
				nummap.put("value", map.get(string));
				list.add(nummap);
				break;
			case "杭州":
				nummap = new HashMap<>();
				nummap.put("lat", Double.parseDouble(hangzhou.split(",",-1)[1]));
				nummap.put("lng", Double.parseDouble(hangzhou.split(",",-1)[0]));
				nummap.put("value", map.get(string));
				list.add(nummap);
				break;
			case "乌鲁木齐":
				nummap = new HashMap<>();
				nummap.put("lat", Double.parseDouble(wulumuqi.split(",",-1)[1]));
				nummap.put("lng", Double.parseDouble(wulumuqi.split(",",-1)[0]));
				nummap.put("value", map.get(string));
				list.add(nummap);
				break;
			case "深圳":
				nummap = new HashMap<>();
				nummap.put("lat", Double.parseDouble(shenzhen.split(",",-1)[1]));
				nummap.put("lng", Double.parseDouble(shenzhen.split(",",-1)[0]));
				nummap.put("value", map.get(string));
				list.add(nummap);
				break;
			case "成都":
				nummap = new HashMap<>();
				nummap.put("lat", Double.parseDouble(chengdu.split(",",-1)[1]));
				nummap.put("lng", Double.parseDouble(chengdu.split(",",-1)[0]));
				nummap.put("value", map.get(string));
				list.add(nummap);
				break;
			
			default:
				break;
			}
		}
		
		JSONArray json =JSONArray.fromObject(list);
		System.out.println(json.toString());
	}

}
