package com.YunTu.TopoLine.SelectDriver;

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


/**
 * 每个id对应的深度
 * @author 84031
 *
 */
public class DeepVisitBolt extends BaseRichBolt {

	 /** 
     *  
     */  
     private static final long serialVersionUID = 1L;  
     private static Logger LOG = LoggerFactory.getLogger(DeepVisitBolt.class);
     private long _lastTime =0;
     private long _intervalTime = 0;
     private long _num = 0;
     private long _intervalNum =0;
  
     OutputCollector collector;  
     Map<String, Integer> counts = new HashMap<String, Integer>();// 每个task实例都会输出
     
     public void prepare(Map map, TopologyContext context, OutputCollector collector) {
    	 this.collector = collector;  
    	 _intervalTime = (long) map.get(OriginalTopology.DEEPBOLT_INTERVAL_TIME);
    	 _intervalNum = (long) map.get(OriginalTopology.DEEPBOLT_INTERVAL_NUM);
     }
    
	public void execute(Tuple tuple) {
		String id = tuple.getStringByField("id");
		Integer count = 0;
		try {
			count = counts.get(id);
			if (count == null) {
				count = 0;
			}
			count++;
			counts.put(id, count);
			long currentTime = System.currentTimeMillis();
			if (_lastTime == 0 || currentTime >= _lastTime + _intervalTime || count>=_num+_intervalNum) {
//				System.out.println(count+"\t"+_num+"\t"+_lastTime+"\t"+currentTime);
				_lastTime = currentTime;
				_num=count;
				if (id!=null) {
					String[] tags = id.split(OriginalTopology.segmentation, -1);
					String dwtvs_task = tags[0];
					String dwtvs_logo = tags[1];
					String dwtvs_name = tags[2];
					int dwtvs_date_year = Integer.parseInt(tags[3]);
					int dwtvs_date_month = Integer.parseInt(tags[4]);
					int dwtvs_date_day = Integer.parseInt(tags[5]);
					int dwtvs_date_hour = Integer.parseInt(tags[6]);
					String dwtvs_type = tags[7];
					// emit安全
					this.collector.emit(new Values(dwtvs_task, dwtvs_logo, dwtvs_name, dwtvs_date_year, dwtvs_date_month,
							dwtvs_date_day, dwtvs_date_hour, count, dwtvs_type));// id
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("DeepVisitBolt is failure.date:id:" + id + ",Count" + count);
			this.collector.fail(tuple);
		}
		this.collector.ack(tuple);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		 declarer.declare(new Fields("dwtvs_task","dwtvs_logo", "dwtvs_name","dwtvs_date_year","dwtvs_date_month","dwtvs_date_day","dwtvs_date_hour","dwtvs_count_num","dwtvs_type"));  
	}
	
}
