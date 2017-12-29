package com.YunTu.BigTable.flyTable;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class PeopleCount {

	public static void main(String[] args) throws Exception {
		BufferedReader bufferedReader = new BufferedReader(
				new InputStreamReader(
						new FileInputStream("src/main/java/province.json"), "utf-8"));
		StringBuffer strbuffer = new StringBuffer();
		while (bufferedReader.ready()) {
			String line = bufferedReader.readLine();
			if (line!=null && line!="") {
				strbuffer.append(line);
			}
		}
		bufferedReader.close();
		
		String data = strbuffer.toString();
		JSONObject json = JSONObject.fromObject(data);
		ArrayList<Map<String, Object>> list = new ArrayList<Map<String,Object>>();
		if (json!=null) {
			JSONArray jSONArray = json.getJSONArray("rows");
			Iterator<JSONObject> iterator = jSONArray.listIterator();
			while (iterator.hasNext()) {
				JSONObject jsonObject = (JSONObject) iterator.next();
				String adcode = jsonObject.getString("adcode");
				int peopleCount = jsonObject.getInt("people_count_2010");
				Map<String, Object> tmpMap = new HashMap<String, Object>();
				tmpMap.put("adcode", adcode);
				tmpMap.put("value", peopleCount);
				list.add(tmpMap);
			}
			System.out.println(jSONArray.size());
			JSONArray newJson = JSONArray.fromObject(list);
			System.out.println(newJson.size());
			System.out.println(newJson);
		}
	}
}
