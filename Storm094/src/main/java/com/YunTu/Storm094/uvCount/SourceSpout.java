package com.YunTu.Storm094.uvCount;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 本地读取数据按条打散输出
 * @author 84031
 *
 */
public class SourceSpout extends BaseRichSpout {

	 /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<String>();// 原子操作线程安全  
     private SpoutOutputCollector collector;  
     
     BufferedReader br;
     public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
    	 //本地测试
    	 this.collector=collector;
		try {
			 br = new BufferedReader(new
			 FileReader("/home/sy/t_dataab"));//C:\\Users\\84031\\Documents\\t_dataab
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} 
     }

	public void nextTuple() {
		try {
			while (br.ready()) {
				String line=br.readLine();
				this.collector.emit(new Values(line));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		 declarer.declare(new Fields("line"));  

	}

}
