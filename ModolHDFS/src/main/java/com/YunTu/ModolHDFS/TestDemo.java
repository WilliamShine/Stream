package com.YunTu.ModolHDFS;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class TestDemo {
	private static Logger LOG = LoggerFactory.getLogger(TestDemo.class);

	 //创建新文件
    public static void createFile(String dst , byte[] contents) throws IOException{
    	System.setProperty("HADOOP_USER_NAME", "root");
    	String hdfsIp = "hdfs://node01.cdh1:8020";
		URI uri = URI.create(hdfsIp);
		Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(uri, conf);
        Path dstPath = new Path(dst); //目标路径
        //打开一个输出流
        FSDataOutputStream outputStream = fs.create(dstPath);
        outputStream.write(contents);
        outputStream.close();
        fs.close();
        System.out.println("文件创建成功！");
    }
    
    //上传本地文件
    public static void uploadFile(String src,String dst) throws IOException{
    	String hdfsIp = "hdfs://node01.cdh1:8020";
		URI uri = URI.create(hdfsIp);
		Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(uri, conf);
        Path srcPath = new Path(src); //原路径
        Path dstPath = new Path(dst); //目标路径
        //调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认为false
        fs.copyFromLocalFile(false,srcPath, dstPath);
        
        //打印文件路径
        System.out.println("Upload to "+conf.get("fs.default.name"));
        System.out.println("------------list files------------"+"\n");
        FileStatus [] fileStatus = fs.listStatus(dstPath);
        for (FileStatus file : fileStatus) 
        {
            System.out.println(file.getPath());
        }
        fs.close();
    }
    
    //文件重命名
    public static void rename(String oldName,String newName) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path oldPath = new Path(oldName);
        Path newPath = new Path(newName);
        boolean isok = fs.rename(oldPath, newPath);
        if(isok){
            System.out.println("rename ok!");
        }else{
            System.out.println("rename failure");
        }
        fs.close();
    }
    //删除文件
    public static void delete(String filePath) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(filePath);
        boolean isok = fs.deleteOnExit(path);
        if(isok){
            System.out.println("delete ok!");
        }else{
            System.out.println("delete failure");
        }
        fs.close();
    }
    
    //创建目录
    public static void mkdir(String path) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(path);
        boolean isok = fs.mkdirs(srcPath);
        if(isok){
            System.out.println("create dir ok!");
        }else{
            System.out.println("create dir failure");
        }
        fs.close();
    }
    
    //读取文件的内容
    public static void readFile(String filePath) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(filePath);
        InputStream in = null;
        try {
            in = fs.open(srcPath);
            IOUtils.copyBytes(in, System.out, 4096, false); //复制到标准输出流
        } finally {
            IOUtils.closeStream(in);
        }
    }
    
    public Map<String, Set<String>> getUrl() {
		Map<String, Set<String>> map = new HashMap<String, Set<String>>();
		Set<String> XMset = new HashSet<String>();
		String hdfsIp = "hdfs://node01.cdh1:8020";
		URI uri = URI.create(hdfsIp);
		Configuration conf = new Configuration();
		Path path = new Path("/user/shine/1407714309205630_76131_10214/t_label");
		try {
			FileSystem fs = FileSystem.get(uri, conf);
			boolean exitFlag = fs.exists(path);
			FSDataInputStream hdfsIn;
			if (exitFlag) {
				System.out.println(exitFlag);
				BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(path), "UTF-8"));
				while (bufferedReader.ready()) {
					String[] lines = bufferedReader.readLine().split("\t", 7);
					String hit_tag = lines[5];
					if (hit_tag.indexOf("/声量/") != -1) {
						if (hit_tag.indexOf("/小米") != -1) {
							XMset.add(lines[3].replace("\"", ""));
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
    
    public static void main(String[] args) throws IOException {
    	System.setProperty("HADOOP_USER_NAME", "root");
    	//文件创建输出
    	//createFile("/user/shine/test1", "创建1".getBytes());
        //测试上传文件
        //uploadFile("D:\\c.txt", "/user/hadoop/test/");///user/shine/1407714309205630_76131_10214/t_label
        //测试创建文件
        /*byte[] contents =  "hello world 世界你好\n".getBytes();
        createFile("/user/hadoop/test1/d.txt",contents);*/
        //测试重命名
        //rename("/user/hadoop/test/d.txt", "/user/hadoop/test/dd.txt");
        //测试删除文件
        //delete("test/dd.txt"); //使用相对路径
        //delete("test1");    //删除目录
        //测试新建目录
        //mkdir("test1");
        //测试读取文件
//        readFile("test1/d.txt");
    	Map<String, Set<String>> map = new TestDemo().getUrl();
		Set<String> set = map.get("/声量/小米");
		for (String string : set) {
			System.out.println("A"+string);
		}
    }
}

