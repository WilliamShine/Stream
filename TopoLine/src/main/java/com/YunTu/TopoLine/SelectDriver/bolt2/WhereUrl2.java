package com.YunTu.TopoLine.SelectDriver.bolt2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




public class WhereUrl2 {
	private static Logger LOG = LoggerFactory.getLogger(WhereUrl2.class);
	
	public Map<String, Set<String>> getUrl(String hdfsIp,String pathStr,int hit_tagN,int url_crcN) {
		Map<String, Set<String>> map = new HashMap<String, Set<String>>();
		Set<String> XMset = new HashSet<String>();
		
		System.setProperty("HADOOP_USER_NAME", "root");//权限
		URI uri = URI.create(hdfsIp);
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");//解决maven打包错误覆盖
		Path path = new Path(pathStr);
		try {
			FileSystem fs = FileSystem.get(uri, conf);
			boolean exitFlag = fs.exists(path);
			if (exitFlag) {
				BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(path), "UTF-8"));
				while (bufferedReader.ready()) {
					String[] lines = bufferedReader.readLine().split("\t", 7);
					String hit_tag = lines[hit_tagN];//5
					if (hit_tag.indexOf("/声量/") != -1 && hit_tag.indexOf("/小米") != -1) {
						XMset.add(lines[url_crcN].replace("\"", ""));//3
					}
						/*
						 * else if (hit_tag.indexOf("/夏普") != -1) {
						 * set.add(lines[3].replace("\"", "")); } else if
						 * (hit_tag.indexOf("/") != -1) {
						 * set.add(lines[3].replace("\"", "")); } else if
						 * (hit_tag.indexOf("/小米") != -1) {
						 * set.add(lines[3].replace("\"", "")); } else if
						 * (hit_tag.indexOf("/小米") != -1) {
						 * set.add(lines[3].replace("\"", "")); } else {
						 * 
						 * }
						 */
				}
				map.put("/声量/小米", XMset);
				if (bufferedReader != null) {
					bufferedReader.close();
				}
			} else {
				LOG.error("错误的HDFS文件读取地址，请检查配置："+path);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return map;
	}
	
	public Map<String,Integer> getUname() {
		Map<String,Integer> map = new HashMap<String,Integer>();
			try {
				BufferedReader bufferedReader = new BufferedReader(new FileReader(new File("dw_tv_uname")));
				while (bufferedReader.ready()) {
					String[] lines = bufferedReader.readLine().split("\t", -1);
					if (lines.length>=3) {
						String dwtvu_type = lines[2];
						if (dwtvu_type .equals("\"/neb_data/\"")) {
							if (map.containsKey(lines[1])) {
								int num = map.get(lines[1]);
								map.put(lines[1], ++num);
							} else {
								map.put(lines[1], 1);
							}
						} 
					}
				}
				if (bufferedReader != null) {
					bufferedReader.close();
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		return map;
	}
	
	/*@Test
	public void test1() {
		String hdfsIp = "hdfs://node01.cdh1:8020";
		String pathStr = "/user/shine/1407714309205630_76131_10214/t_label";
		Map<String, Set<String>> map = new WhereUrl2().getUrl(hdfsIp,pathStr);
		Set<String> set = map.get("/声量/小米");
		for (String string : set) {
			System.out.println(string);
		}
	}*/
	
	/*@Test
	public void test2() {
		Map<String,Integer> map = new WhereUrl().getUname();
		Set<String> set =map.keySet();
		System.out.println("------------------");
		for (String string : set) {
			System.out.println(string);
		}
		for (String key : set) {
			System.out.println(key+"\t\t"+map.get(key));
		}
	}
	
	@Test
	public void test3() {
		System.out.println("\"/neb_data/\"");//"/neb_data/"
	}*/
	
	/*@Test
	public void test4() {
		String hdfsIp="hdfs://node01.cdh1:8020";
		URI uri=URI.create(hdfsIp);
		
		Configuration conf=new Configuration();
		Path path=new Path("/ShangYue");
		try {
			FileSystem fs=FileSystem.get(uri, conf);
			boolean flag=fs.exists(path);
			System.out.println(flag);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}*/
	/*@Test
	public void test5() {
		throw new RuntimeException("adadada");
		try {
		 * throw new RuntimeException("adadada");
		} catch (Exception e) {
			System.out.println("aaa");
		}

	}*/
}
