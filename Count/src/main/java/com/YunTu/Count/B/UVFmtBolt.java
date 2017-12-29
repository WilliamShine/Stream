package com.YunTu.uvCount.B;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


/**
 * 切分格式转换
 * @author 84031
 *
 */
public class UVFmtBolt extends BaseRichBolt {

	 /** 
     *  
     */  
     private static final long serialVersionUID = 1L;  
  
     OutputCollector collector;  
     public void prepare(Map map, TopologyContext context, OutputCollector collector) {
    	 this.collector = collector;  
    	 
     }
     
	public void execute(Tuple tuple) {
        String line = tuple.getString(0); 
        String id = line.split("\t")[0];  
          
        try {  
             this.collector.emit(new Values(id));  
             this.collector.ack(tuple);  
        } catch (Exception e) {  
             e.printStackTrace();  
             this.collector.fail(tuple);  
        }  

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		 declarer.declare(new Fields("id"));  

	}

}
