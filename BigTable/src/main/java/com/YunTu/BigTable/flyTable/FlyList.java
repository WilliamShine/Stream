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
public class FlyList {

	public static void main(String[] args) throws Exception {
		ArrayList<Map<String, Object>> list = new ArrayList<>();
		
		BufferedReader bufferedReader = new BufferedReader(new FileReader("src/main/java/flydata.csv"));
		Map<String, Object> map = null;
		while (bufferedReader.ready()) {
			String line =bufferedReader.readLine();
			String[] lines = line.split(",", -1);
			map = new HashMap<>();
			map.put("code", lines[1]);
			map.put("from", lines[4]);
			map.put("goTo", lines[7]);
			map.put("startTime", lines[6]);
			map.put("Company", lines[0]);
			list.add(map);
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

		JSONArray json = JSONArray.fromObject(list);
		System.out.println(json);
	}

}
