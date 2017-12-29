package com.YunTu.LineStream.SimpleUVDriver.DeepVisit;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DeepVisitBolt extends BaseRichBolt {

	 /** 
    *  
    */  
    private static final long serialVersionUID = 1L;  
    private static Logger LOG = LoggerFactory.getLogger(DeepVisitBolt.class);
 
    OutputCollector collector; 
    Map<String, Long> countMap ;
    String id;
    Long deep;
    
	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		countMap = new HashMap<String, Long>();
	}
   
	public void execute(Tuple tuple) {
		try {
			execution(tuple);
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("DeepVisitBolt is failure.date ######:tuple:" + tuple );
			this.collector.fail(tuple);
		}
		this.collector.ack(tuple);
	}
	
	private void execution(Tuple tuple) {
		id = tuple.getStringByField("id");
		deep= countMap.get(id);
		if (deep == null) {
			deep = 0L;
		}
		deep++;
		countMap.put(id, deep);
		this.collector.emit(new Values(id,deep));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		 declarer.declare(new Fields("id","Deep"));  
	}
	
}
