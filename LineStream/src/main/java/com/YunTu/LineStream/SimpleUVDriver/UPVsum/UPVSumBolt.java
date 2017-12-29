package com.YunTu.LineStream.SimpleUVDriver.UPVsum;

import java.sql.Timestamp;
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



public class UPVSumBolt extends BaseRichBolt {

	/** 
	*  
	*/
	private static final long serialVersionUID = 1L;
	private static Logger LOG = LoggerFactory.getLogger(UPVSumBolt.class);
	OutputCollector collector;
	Map<String, Long> countsMap ;
	String id;
	Long deep;
	Long pv;
	Long uv;
	
	long _lastUpdateMs = System.currentTimeMillis();
	long _stateUpdateIntervalMs = 2000;
	long nowTime ;
	long diffWithNow ;
	
	Boolean log = true;

	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		countsMap = new HashMap<String, Long>();
		pv = 0L;
		uv = 0L;
		
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
			deep = tuple.getLongByField("Deep");
			if (countsMap.containsKey(id)) {
				pv = pv + deep - countsMap.get(id);
			}else {
				pv +=deep;
				uv+=1;
			}
			countsMap.put(id, deep);// 汇总每个id 对应的深度,这里可通过map或者mysql作为去重的持久化操作
			
			System.out.println("==============>> UV="+uv +"     PV:"+pv);
			
			/*ShareResource.putValue(id,count);
			ShareResource.putValue("UV", uv);
			ShareResource.putValue("PV", pv);*/
			
			/*if (log) {
				Thread thread = new Thread(new RootNodeChangeThread(countsMap,collector));
				thread.setPriority(1);
				thread.start();//多线程,抢线程资源
				log = false;
			}*/
			long currentTime = System.currentTimeMillis();
			if ( currentTime >= nowTime + _stateUpdateIntervalMs ) {
//				System.out.println(count+"\t"+_num+"\t"+_lastTime+"\t"+currentTime);
				nowTime = currentTime;
			// emit安全问题	1.追求当时无误差数据采用多线程或分布式缓存。2.追求流式数据，当时低误差采用间隔输出。//new Timestamp(System.currentTimeMillis())
			this.collector.emit(new Values("SimpleUVcount",pv,uv,12L));
			}
	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("task_Number","total_pv","total_uv","last_update"));
	}
}
