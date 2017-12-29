package com.YunTu.Storm094.uvCount.A;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

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
