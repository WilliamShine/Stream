package com.YunTu.ModolHDFS;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    		String hdfsIp="hdfs://node01.cdh1:8020";
    		URI uri=URI.create(hdfsIp);
    		
    		Configuration conf=new Configuration();
    		Path path=new Path("/user");
    		try {
    			FileSystem fs=FileSystem.get(uri, conf);
    			boolean flag=fs.exists(path);
    			System.out.println(flag);
    		} catch (IOException e) {
    			e.printStackTrace();
    	}
    }
}
