package com.YunTu.LineStream.DayUVDriver.UPVsum;

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
	
	/*long _lastUpdateMs = System.currentTimeMillis();
	long _stateUpdateIntervalMs = 2000;
	long nowTime ;
	long diffWithNow ;
	
	Boolean log = true;*/

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
				pv+=deep;
				uv++;
			}
			countsMap.put(id, deep);// 汇总每个id 对应的深度,这里可通过map或者mysql作为去重的持久化操作
			
			//System.out.println("==============>> UV="+uv +"     PV:"+pv);
			/*ShareResource.putValue(id,count);
			ShareResource.putValue("UV", uv);
			ShareResource.putValue("PV", pv);*/
			
			/*if (log) {
				Thread thread = new Thread(new RootNodeChangeThread(countsMap,collector));
				thread.setPriority(1);
				thread.start();//多线程,抢线程资源
				log = false;
			}*/
			
			/*String[] tags = id.split(DemoDriver.segmentation, -1);
			String dwtvs_task = tags[0];
			String dwtvs_logo = tags[1];
			String dwtvs_name = tags[2];
			int dwtvs_date_year = Integer.parseInt(tags[3]);
			int dwtvs_date_month = Integer.parseInt(tags[4]);
			int dwtvs_date_day = Integer.parseInt(tags[5]);
			int dwtvs_date_hour = Integer.parseInt(tags[6]);
			String dwtvs_type = tags[7];*/
				// emit安全问题
			System.out.println(uv+pv);
			this.collector.emit(new Values("SimpleUVcount",pv,uv,12L));
	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("task_Number","total_pv","total_uv","last_update"));
	}
	
	
	//实时搜集顺序传输
	/*	
	// 访问日期若不是以今天开头并且访问日期大于当前日期,则计算新的一天的UV  
    String currDate = sdf.format(new Date());  
    try {  
         Date accessDate = sdf.parse(dateSid.split("_")[0]);  
         if (!dateSid.startsWith(currDate) && accessDate.after(new Date())) {  
              counts.clear();  
         }  
    } catch (ParseException e1) {  
         e1.printStackTrace();  
    }  
    counts.put(dateSid, count);// 汇总每个访客  对应的PV数,这里可以通过map或者hbase作为去重的持久化操作  
      
    for (Map.Entry<String, Integer> e : counts.entrySet()) {  
         if(dateSid.split("_")[0].startsWith(currDate)){  
              uv++;  
              pv+=e.getValue();  
         }  
    }  */
}
