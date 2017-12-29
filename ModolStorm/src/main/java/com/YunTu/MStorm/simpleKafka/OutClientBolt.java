package com.YunTu.MStorm.simpleKafka;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
/**
 * 打印输出bolt，取tuple的是arrayList的第一个标识的字段
 * 主要用于接收kafaka发过来的StringScheme()后的数据，因为不知道其标识，查源码就知道了
 * @author 84031
 *
 */
public class OutClientBolt extends BaseRichBolt {
	private OutputCollector collector;
	
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector=collector;
	}

	public void execute(Tuple tuple) {
		String line=tuple.getStringByField("myscheme");//查StringScheme()知道其Field设置标识是str
//		String line=tuple.getString(0);// 取得是第一个字段，也就是第一个标识字段，是一个arrayList的结构
		//两种取法，取数组第几个标识，和取哪个标识
		//没有使用反馈机制，会重打
		System.out.println(line);
		this.collector.emit(new Values(line+"####"));
		this.collector.ack(tuple);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));//这里，给Kafkabolt输出的字段不能是两个，且必须是"message"字段
	}

}
