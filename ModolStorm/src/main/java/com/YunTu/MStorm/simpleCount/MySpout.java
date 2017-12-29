package com.YunTu.MStorm.simpleCount;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MySpout extends BaseRichSpout {
	private SpoutOutputCollector collector ;
	
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector=collector;
	}
	
	public void nextTuple() {
		String[] lines = {"aaaa","bbbbb","ccccc"};
		for (String string : lines) {
			collector.emit(new Values(string));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("1号Spout标识"));
	}

}
