package com.YunTu.Storm111.uvCount.A;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;


public class Tsetbolt extends BaseRichBolt {

	public void execute(Tuple tuple) {
		 String line = tuple.getString(0);
		 System.out.println(line);
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		
		
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		
	}

}
