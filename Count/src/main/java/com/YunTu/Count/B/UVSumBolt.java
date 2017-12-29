package com.YunTu.uvCount.B;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
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
     
     Statement stat=null;     
	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		this.collector = collector;  
		
		String mysqlUrl = "jdbc:mysql://106.14.248.228:23306/data_warehouse";
		String userName = "weiwei.wu";
		String password = "Miweiwei20170711@";
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection conn = DriverManager.getConnection(mysqlUrl, userName,password);
			stat = conn.createStatement();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void execute(Tuple tuple) {
		 pv = 0;  
         uv = 0;  
           
         try {
			String dateSid = tuple.getStringByField("cid");  
			 Integer count = tuple.getIntegerByField("count");  
			 counts.put(dateSid, count);// 汇总每个id  对应的深度,这里可通过map或者mysql作为去重的持久化操作  
			   
			for (Map.Entry<String, Integer> e : counts.entrySet()) {
				uv++;
				pv += e.getValue();
			} 
			 //保存到HBase或者数据库中  
			//connection
			
			String sql = "update sy_count set UV="+uv+",PV="+pv+" where id=20171219";
			int num = stat.executeUpdate(sql);
			if (num==1) {
				System.out.println("向mysql当中数据插入成功，num="+num+"。pv="+pv+"。uv="+uv);
			} else {
				System.out.println("失败!向mysql当中数据插入失败，num="+num+"。pv="+pv+"。uv="+uv);
			}
			 this.collector.emit(new Values(pv, uv));//id 深度
             this.collector.ack(tuple);  
		} catch (Exception e) {
			e.printStackTrace();
		} 
         

	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("UV","PV"));
	}

}
