package com.YunTu.ModolKafkaPC.KafkaReadHDFS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * HDFS-Kafka 提供两种方法，读取单个文件和读取目录下所有文件，打入kafka中
 * @author 84031
 *
 */
public class HDFSReaderProducer {

    public static void main(String[] args) {
        Properties props = new Properties();//47.100.9.7,47.100.9.241,47.100.6.154
        props.put("bootstrap.servers", "47.100.9.7:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String,String> producer = new KafkaProducer<String,String>(props);

        HDFSReader(producer);
        //HDFSListDirectory(producer);
        producer.close();
    }
    
    /**
     * 文件读取
     * @return
     */
    private static void HDFSReader(Producer<String, String> producer) {
    	String hdfsIp = "hdfs://101.201.68.72:9000";
		URI uri = URI.create(hdfsIp);

		Configuration conf = new Configuration();
		Path path = new Path("/sy/t_dataab");
		BufferedReader bufferReader=null;
		try {
			FileSystem fs = FileSystem.get(uri, conf);
			FSDataInputStream fsin= fs.open(path);
			bufferReader= new BufferedReader(new InputStreamReader(fsin, "utf-8"));
			while (bufferReader.ready()) {
				producer.send(new ProducerRecord<String, String>("sytest", bufferReader.readLine()));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
    
    /**
     * 目录读取
     */
    private static void HDFSListDirectory(Producer<String, String> producer) {
    	String hdfsIp = "hdfs://101.201.68.72:9000";
		URI uri = URI.create(hdfsIp);

		Configuration conf = new Configuration();
		Path path = new Path("/sy");
		BufferedReader bufferReader=null;
		try {
			FileSystem fs = FileSystem.get(uri, conf);
			fs.listStatus(path);
			RemoteIterator<?> remoteIterator = fs.listFiles(path, true);
			while (remoteIterator.hasNext()) {
				FileStatus type = (FileStatus) remoteIterator.next();
				FSDataInputStream fSDataInputStream = fs.open(type.getPath());
				bufferReader= new BufferedReader(new InputStreamReader(fSDataInputStream, "utf-8"));
				while (bufferReader.ready()) {
					producer.send(new ProducerRecord<String, String>("sytest", bufferReader.readLine()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
