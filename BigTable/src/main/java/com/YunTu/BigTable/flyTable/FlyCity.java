package com.YunTu.BigTable.flyTable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import net.sf.json.JSONArray;

/**
 * 航空目的地排名轮播图
 * @author 84031
 *
 */
public class FlyCity {

	public static void main(String[] args) throws Exception {
		ArrayList<Map<String, Object>> list = new ArrayList<>();
		
		BufferedReader bufferedReader = new BufferedReader(new FileReader("src/main/java/flydata.csv"));
		Map<String, Integer> map = new HashMap<>();
		while (bufferedReader.ready()) {
			String line =bufferedReader.readLine();
			String size = line.split(",", -1)[7];
			Integer num = map.get(size);
			if (num!=null) {
				map.put(size, ++num);
			} else {
				map.put(size, 1);
			}
		}
		bufferedReader.close();
		Set<String> set= map.keySet();
		int sum=0;
		/*for (String string : set) {
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
			flymap.put("area", string);
			flymap.put("pv", map.get(string));
			flymap.put("attribute", "正常到达");
			list.add(flymap);
		}
		
		
		JSONArray json = JSONArray.fromObject(list);
		System.out.println(json);
	}

}
