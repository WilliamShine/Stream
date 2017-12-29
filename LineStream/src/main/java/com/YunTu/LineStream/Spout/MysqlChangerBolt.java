package com.YunTu.LineStream.Spout;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MysqlChangerBolt extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	OutputCollector _collector;
	public static Logger LOG = LoggerFactory.getLogger(MysqlChangerBolt.class);
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this._collector = collector;
		
	}

	@Override
	public void execute(Tuple input) {
		System.out.println(input.getLongByField("UV")+"---"+input.getLongByField("PV"));
		_collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
