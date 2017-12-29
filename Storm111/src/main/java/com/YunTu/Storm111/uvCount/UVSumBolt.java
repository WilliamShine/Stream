package com.YunTu.Storm111.uvCount;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;


/**
 * 单线程汇总最终结果
 * @author 84031
 *
 */
public class UVSumBolt extends BaseRichBolt {

     /** 
     *  
     */  
     private static final long serialVersionUID = 1L;  
     OutputCollector collector;  
     Map<String, Integer> counts = new HashMap<String, Integer>();  
     int pv = 0;  
     int uv = 0; 
    //<k,v> 每个id  对应的深度
    BufferedWriter bw = null;
	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		this.collector = collector;  
		/*try {
			bw = new BufferedWriter(new FileWriter("/home/shine/result"));
		} catch (IOException e) {
			e.printStackTrace();
		}*/
	}
	
	public void execute(Tuple tuple) {
		 pv = 0;  
         uv = 0;  
           
         String dateSid = tuple.getStringByField("cid");  
         Integer count = tuple.getIntegerByField("count");  
         counts.put(dateSid, count);// 汇总每个id  对应的深度,这里可通过map或者mysql作为去重的持久化操作  
           
		for (Map.Entry<String, Integer> e : counts.entrySet()) {
			uv++;
			pv += e.getValue();
		} 
         //保存到HBase或者数据库中  
         System.out.println("pv数为"+pv+",uv数为"+uv); 
        /* try {
			bw.write("pv数为"+pv+",uv数为"+uv);
			bw.flush();
		} catch (IOException e1) {
			e1.printStackTrace();
		}*/

	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		System.out.println("pv数为"+pv+",uv数为"+uv);
	}

}
