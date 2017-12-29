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
 * 航空时间柱状分布图
 * @author 84031
 *
 */
public class FlyTime {

	public static void main(String[] args) throws Exception {
		ArrayList<Map<String, Object>> list = new ArrayList<>();
		
		BufferedReader bufferedReader = new BufferedReader(new FileReader("src/main/java/flydata.csv"));
		Map<String, Integer> map = new HashMap<>();
		while (bufferedReader.ready()) {
			String line =bufferedReader.readLine();
			String size = line.split(",", -1)[6].split(":",-1)[0];
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
		for (int i = 0; i < 24; i++) {
			if (set.contains(i+"")) {
				flymap = new HashMap<>();
				flymap.put("x", i+"点钟");
				flymap.put("y", map.get(i+""));
				flymap.put("z", map.get(i+""));
				list.add(flymap);
			} else {
				flymap = new HashMap<>();
				flymap.put("x", i+"点钟");
				flymap.put("y", 0);
				flymap.put("z", 0);
				list.add(flymap);
			}
		}
		
		JSONArray json = JSONArray.fromObject(list);
		System.out.println(json);
	}

}
