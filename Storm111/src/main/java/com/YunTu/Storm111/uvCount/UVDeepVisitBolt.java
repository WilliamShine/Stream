package com.YunTu.Storm111.uvCount;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


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
