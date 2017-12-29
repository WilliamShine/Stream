package com.YunTu.BigTable;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class JsonDemo {

//	JSONArray	[]
//	JSONObject  {}
	@Test
	public void Demo1() {
		//KV
		String line = "{\"key\":\"value\",\"key1\":\"value1\"}";
		JSONObject json = JSONObject.fromObject(line);
		Object obj = json.get("key");
		System.out.println(obj.toString());
		System.out.println(json.toString());
		//Array
		String line1 = "[\"abc\",18,10,true]";
		JSONArray jsonA = JSONArray.fromObject(line1);
		Object value = jsonA.get(0);
		System.out.println(value);
		
		//综合
		String comLine =  "{\"key\":\"value\",\"key1\":[\"abc\",18,10,true]}";
		JSONObject comObj = JSONObject.fromObject(comLine);
		System.out.println(comObj.get("key1"));
	}
	
	@Test
	public void JsonCollectionDemo() {
		//数组转json
		ArrayList list = new ArrayList();
		list.add(190);
		list.add("String");
		list.add(true);
 
		JSONArray jsonList = JSONArray.fromObject(list);
		System.out.println(jsonList);
		
		//map转json
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("CC", 20);
		map.put("xiaoMin", "姓名");
		map.put("Bobi", true);
		map.put("list", list);
		JSONObject jsonObj = JSONObject.fromObject(map);
		System.out.println(jsonObj.toString());
		
	}
	
	@Test
	public void test2() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		try {
			//截取成时间
			sdf.parse("2017-11-27 10:49:01");//Mon Nov 27 00:00:00 CST 2017
			//转型yyyy-mm-dd字符
			sdf.format(sdf.parse("2017-11-27 10:49:01"));//2017-11-27
			
			Calendar calendar = Calendar.getInstance();
            calendar.setTime(sdf.parse("2017-11-27 10:49:01"));
			
			System.out.println(calendar.get(Calendar.YEAR));
		} catch (ParseException e) {
			e.printStackTrace();
		}

	}

}
