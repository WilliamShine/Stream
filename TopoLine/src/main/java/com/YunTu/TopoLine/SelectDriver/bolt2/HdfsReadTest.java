package com.YunTu.TopoLine.SelectDriver.bolt2;

import java.util.Map;
import java.util.Set;

public class HdfsReadTest {

	public static void main(String[] args) {
		Map<String, Set<String>> usrmap = null;
		Set<String> set = null;
		usrmap = new WhereUrl2().getUrl("hdfs://node01.cdh1:8020", "/user/shine/1407714309205630_76131_10214/t_label",5,3);
		set = usrmap.get("/声量/小米");
		for (String string : set) {
			System.out.println(string);
		}
	}
	
}
