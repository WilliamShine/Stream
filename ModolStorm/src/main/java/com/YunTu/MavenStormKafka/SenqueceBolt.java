package com.YunTu.MavenStormKafka;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SenqueceBolt extends BaseBasicBolt {

	/**
	 * 输出格式：output:word
	 * 输出位置：控制台，kafkastorm.out文件，下一级bolt
	 */
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String word = (String) tuple.getValue(0);
		String out = "output:" + word;
		System.out.println(out);

		// 写文件
		try {
			DataOutputStream out_file = new DataOutputStream(new FileOutputStream("kafkastorm.out"));
			out_file.writeUTF(out);
			out_file.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		collector.emit(new Values(out));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));
	}

}
