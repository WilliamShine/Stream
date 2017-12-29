package com.YunTu.LineStream;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class CTest {

	@Test
	public void test1() {
		String hit_tag = "智能电视电商声量/声量/产品声量/夏普/45寸/平面/LCD45SF460A/智能电视评论/产品品质/画质/清晰度/正面 智能电视电商声量/声量/品牌声量/夏普/智能电视评论/产品品质/画质/清晰度/正面 智能电视电商声量/声量/产品声量/夏普/45寸/平面/LCD45SF460A/智能电视评论/综合评价/正面 智能电视电商声量/声量/品牌声量/夏普/智能电视评论/综合评价/正面";
		String tag = hit_tag.split("/", 5)[3];
		System.out.println(tag);
	}
	
	@Test
	public void syso() {
		String hit_tag = "aaaa/bbb/cccc/ddd";
		String hit_tags = hit_tag.split("/", 4)[3];
		System.out.println(hit_tags);
	}
	
	@Test
	public void test2() {
		long _lastUpdateMs = 0;
		long _stateUpdateIntervalMs=5000;
		while (true) {
			long nowTime=System.currentTimeMillis();
			long diffWithNow = nowTime - _lastUpdateMs;
			if (diffWithNow > _stateUpdateIntervalMs || diffWithNow < 0) {
				System.out.println(nowTime);
				_lastUpdateMs=nowTime;
			}
		}

	}
	
	@Test
	public void test3() {
		try {
			System.out.println("3dada".matches("-?\\d+"));
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void test4() {
		Map<String, Integer> map = new HashMap<String, Integer>();
		map.put(null, 4);
		map.put("aaa", 3);
		System.out.println(map.size());
		System.out.println(map.get(null));
		

	}
	
	@Test
	public void dateTest() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd"); 
		String currDate = sdf.format(new Date());  
		System.out.println(currDate);

	}
}
