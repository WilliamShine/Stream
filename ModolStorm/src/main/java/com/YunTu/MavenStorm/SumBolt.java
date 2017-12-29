package com.YunTu.MavenStorm;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;


/**
 * 每个id对应的深度
 * @author 84031
 *
 */
public class SumBolt extends BaseRichBolt {

	 /** 
     *  
     */  
     private static final long serialVersionUID = 1L; 
     private long num = 0;
  
     public void prepare(Map map, TopologyContext context, OutputCollector collector) {
    	
     }

	public void execute(Tuple tuple) {
		String line = tuple.getStringByField("id");
 		System.out.println("字符："+line+"。累计数："+(++num));
	}


	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}  
     

}
