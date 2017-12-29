package com.YunTu.BigTable.flyTable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import net.sf.json.JSONArray;

/**
 * 客机分布饼图
 * @author 84031
 *
 */
public class FlyDistrbuder {

	public static void main(String[] args) throws Exception {
		ArrayList<Map<String, Object>> list = new ArrayList<>();
		BufferedReader bufferedReader = new BufferedReader(new FileReader("src/main/java/flydata.csv"));
		Map<String, Integer> map = new HashMap<>();
		while (bufferedReader.ready()) {
			String line =bufferedReader.readLine();
			String size = line.split(",", -1)[3];
			Integer num = map.get(size);
			if (num!=null) {
				map.put(size, ++num);
			} else {
				map.put(size, 1);
			}
		}
		bufferedReader.close();
		Set<String> set= map.keySet();
		/*int sum=0;
		for (String string : set) {
			sum+=map.get(string);
		}
		for (String string : set) {
			map.put(string, map.get(string)*100/sum);
		}
		for (String string : set) {
			System.out.println(string+map.get(string));
		}*/

		Map<String, Object> flymap =null;
		for (String string : set) {
			flymap = new HashMap<>();
			flymap.put("type", string);
			flymap.put("value", map.get(string));
			list.add(flymap);
		}
		
		JSONArray json = JSONArray.fromObject(list);
		System.out.println(json);
	}

}
