package com.YunTu.BigTable.flyTable;

import java.util.ArrayList;
import java.util.HashMap;

import net.sf.json.JSONArray;

public class FlyRounte {

	/**
	 * 应该读取100000_city.json文件获取到所有的经纬值
	 * @param args
	 */
	public static void main(String[] args) {
		String beiJing = "116.405285,39.904989";
		String shanghai="121.472644,31.231706";
		String guangzhou="113.280637,23.125178";
		String shenzhen="114.085947,22.547";
		String hangzhou="120.153576,30.287459";
		String chengdu="104.065735,30.659462";
		String haerbing="126.642464,45.756967";
		String wulumuqi="87.617733,43.792818";
		
		ArrayList<HashMap<String, String>> list=new ArrayList<HashMap<String,String>>();
		
		//北京到上海
		HashMap<String, String> shanghaiMap=new HashMap<>();
		shanghaiMap.put("from", beiJing);
		shanghaiMap.put("to", shanghai);
		list.add(shanghaiMap);
		
		//北京到广州
		HashMap<String, String> gzMap=new HashMap<>();
		gzMap.put("from", beiJing);
		gzMap.put("to", guangzhou);
		list.add(gzMap);
		
		//北京到深圳
		HashMap<String, String> szMap=new HashMap<>();
		szMap.put("from", beiJing);
		szMap.put("to", shenzhen);
		list.add(szMap);
		
		//北京到杭州
		HashMap<String, String> hzMap=new HashMap<>();
		hzMap.put("from", beiJing);
		hzMap.put("to", hangzhou);
		list.add(hzMap);
		
		//北京到成都
		HashMap<String, String> cdMap=new HashMap<>();
		cdMap.put("from", beiJing);
		cdMap.put("to", chengdu);
		list.add(cdMap);
		
		//北京到哈尔滨
		HashMap<String, String> hrbMap=new HashMap<>();
		hrbMap.put("from", beiJing);
		hrbMap.put("to", haerbing);
		list.add(hrbMap);
		
		//北京到乌鲁木齐
		HashMap<String, String> wlmqMap=new HashMap<>();
		wlmqMap.put("from", beiJing);
		wlmqMap.put("to", wulumuqi);
		list.add(wlmqMap);
		
		JSONArray json = JSONArray.fromObject(list);
		System.out.println(json);
	}

}
