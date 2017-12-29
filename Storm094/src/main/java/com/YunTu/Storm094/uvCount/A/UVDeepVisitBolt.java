package com.YunTu.Storm094.uvCount.A;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


/**
 * 每个id对应的深度
 * @author 84031
 *
 */
public class UVDeepVisitBolt extends BaseRichBolt {

	 /** 
     *  
     */  
     private static final long serialVersionUID = 1L;  
  
     OutputCollector collector;  
     Map<String, Integer> counts = new HashMap<String, Integer>();// 每个task实例都会输出
     
     public void prepare(Map arg0, TopologyContext context, OutputCollector collector) {
    	 this.collector = collector;  
    	 
     }
    
	public void execute(Tuple tuple) {
         String id = tuple.getStringByField("id");  
         String key = id;  
         Integer count = 0;  
         try {  
              count = counts.get(key);  
              if (count == null) {  
                   count = 0;  
              }  
              count++;  
              counts.put(key, count);  
              this.collector.emit(new Values(id, count));//id 深度
              this.collector.ack(tuple);  
         } catch (Exception e) {  
              e.printStackTrace();  
              System.err.println("UVDeepVisitBolt is failure.date:id:" + id + ",uvCount" + count);   
              this.collector.fail(tuple);  
         }  

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		 declarer.declare(new Fields("cid", "count"));  

	}

}
