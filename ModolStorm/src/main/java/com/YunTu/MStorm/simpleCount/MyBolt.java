package com.YunTu.MStorm.simpleCount;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class MyBolt extends BaseRichBolt {
	long num = 0;
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		
	}

	public void execute(Tuple tuple) {
		String line = tuple.getStringByField("1号Spout标识");
		System.out.println("字符："+line+"。累计数："+(++num));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
