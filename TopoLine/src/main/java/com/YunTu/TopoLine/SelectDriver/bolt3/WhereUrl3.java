package com.YunTu.TopoLine.SelectDriver.bolt3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;





public class WhereUrl3 {
	private static Logger LOG = LoggerFactory.getLogger(WhereUrl3.class);

	
	public static Map<String, Set<String>> getTLabelUrl(String hdfsIp, String pathStr, int urlN, int tagN) {// t_label_1407714309205630_76131_10214_1
		Map<String, Set<String>> map = new HashMap<String, Set<String>>();
		Set<String> XMset = new HashSet<String>();
		Set<String> XPset = new HashSet<String>();
		Set<String> QTset = new HashSet<String>();

		System.setProperty("HADOOP_USER_NAME", "root");// 权限
		URI uri = URI.create(hdfsIp);
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");// 解决maven打包错误覆盖
		Path path = new Path(pathStr);
		try {
			FileSystem fs = FileSystem.get(uri, conf);
			boolean exitFlag = fs.exists(path);
			if (exitFlag) {
				BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(path), "UTF-8"));
				while (bufferedReader.ready()) {
					String[] lines = bufferedReader.readLine().split("\t", 7);
					String hit_tag = lines[tagN];//5
					if (hit_tag.indexOf("/声量/") != -1) {
						if (hit_tag.indexOf("/小米") != -1) {
							XMset.add(lines[urlN].replace("\"", ""));//3
						} else if (hit_tag.indexOf("/夏普") != -1) {
							XPset.add(lines[urlN].replace("\"", ""));
						} /*
							 * else if (hit_tag.indexOf("/")!=-1) {
							 * set.add(lines[3].replace("\"", "")); } else if
							 * (hit_tag.indexOf("/小米")!=-1) {
							 * set.add(lines[3].replace("\"", "")); } else if
							 * (hit_tag.indexOf("/小米")!=-1) {
							 * set.add(lines[3].replace("\"", "")); }
							 */else {
							QTset.add(lines[urlN].replace("\"", ""));
						}
					}
				}
				map.put("/声量/小米", XMset);
//				map.put("/声量/夏普", XPset);
//				map.put("/声量/其他", QTset);
				if (bufferedReader != null) {
					bufferedReader.close();
				}
			} else {
				LOG.error("错误的t_labelHDFS文件读取地址，请检查配置：" + path);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return map;
	}
	
	public static Set<String> getTDataUrl(String hdfsIp, String pathStr, int urlN, int typeN) {// t_data_1407714309205630_76131_10214_1,1
		Set<String> set = new HashSet<String>();

		System.setProperty("HADOOP_USER_NAME", "root");// 权限
		URI uri = URI.create(hdfsIp);
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");// 解决maven打包错误覆盖
		Path path = new Path(pathStr);
		try {
			FileSystem fs = FileSystem.get(uri, conf);
			boolean exitFlag = fs.exists(path);
			if (exitFlag) {
				BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(path), "UTF-8"));
				while (bufferedReader.ready()) {
					String[] lines = bufferedReader.readLine().split("\t", -1);
					String rel_type = lines[typeN];//2
					if (rel_type.equals("m") || rel_type.equals("q")) {
						String url_crc = lines[urlN].replace("\"", "");//0
						set.add(url_crc);
					}
				}
				if (bufferedReader != null) {
					bufferedReader.close();
				}
			} else {
				LOG.error("错误的t_dataHDFS文件读取地址，请检查配置：" + path);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return set;
	}
	
	
	public Map<String, Set<String>> getN2ResultUrl(String t_data_hdfsIP,String t_data_PATH,int dt3_t_data_url_crc,int dt3_t_data_rel_type,
			String t_label_hdfsIP,String t_label_PATH,int dt3_t_label_url_crc,int dt3_t_label_hit_tag) {
		Set<String> dataUrlSet = getTDataUrl(t_data_hdfsIP,t_data_PATH,dt3_t_data_url_crc,dt3_t_data_rel_type);
		//"hdfs://node01.cdh1:8020", "/user/shine/1407714309205630_76131_10214/t_data_3", 2,0);
		Map<String, Set<String>> labelMap = getTLabelUrl(t_label_hdfsIP,t_label_PATH,dt3_t_label_url_crc,dt3_t_label_hit_tag);
		//"hdfs://node01.cdh1:8020", "/user/shine/1407714309205630_76131_10214/t_label", 5,3);
		Set<String> set = labelMap.keySet();
		for (String key : set) {
			Set<String> result = labelMap.get(key);
			result.retainAll(dataUrlSet);
		}
		
		return labelMap;
	}
	
	/*@Test
	public void test1() {
		Map<String,Set<String>> map =WhereUrl3.getUrl("t_label_1407714309205630_76131_10214_1",5,3);
		Set<String> set = map.keySet();
		for (String string : set) {
			Set<String> kSet = map.get(string);
			Iterator<String> it =kSet.iterator();
			System.out.println(kSet.size());
			while (it.hasNext()) {
				String string2 = (String) it.next();
				System.out.print(string2+"\t");
			}
			System.out.println("\n---------");
		}
		Set<String> set = map.get("小米");
		if (set.contains("7082874394290368780")) {
			System.out.println("包含");
		} else {
			System.out.println("不包含");
		}
	}*/
	
}
