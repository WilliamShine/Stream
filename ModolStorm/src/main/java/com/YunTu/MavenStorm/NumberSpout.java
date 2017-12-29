package com.YunTu.MavenStorm;

import java.util.Map;

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
public class NumberSpout extends BaseRichSpout {


	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector ;

	public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
		this.collector=collector;
		
	}
	
	public void nextTuple() {
		String[] lines = {"aaaa","bbbbb","ccccc"};
		for (String string : lines) {
			collector.emit(new Values(string));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id"));
		
	}


}
