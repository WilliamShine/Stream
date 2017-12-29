package com.YunTu.Storm094.uvCount;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


public class UVTopology {

	public static final String SPOUT_ID = SourceSpout.class .getSimpleName();  
    public static final String UVFMT_ID = UVFmtBolt.class .getSimpleName();  
    public static final String UVDEEPVISITBOLT_ID = UVDeepVisitBolt.class .getSimpleName();  
    public static final String UVSUMBOLT_ID = UVSumBolt.class .getSimpleName();  
    
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();  
        builder.setSpout( SPOUT_ID , new SourceSpout(), 1);  
        // 切分格式化数据源： 格式： 2017_05_13686666420349405689
        builder.setBolt( UVFMT_ID , new UVFmtBolt(), 4).shuffleGrouping(SPOUT_ID );  
        // 统计每个线程 对应的 UV ,格式： 20151010_ABYH6Y4V4SCV 4  
        builder.setBolt( UVDEEPVISITBOLT_ID , new UVDeepVisitBolt(), 4).fieldsGrouping( UVFMT_ID,  
                new Fields("id"));  
        // 单线程汇总  
        builder.setBolt( UVSUMBOLT_ID , new UVSumBolt(), 1).shuffleGrouping(UVDEEPVISITBOLT_ID );  

       
       Map<String, Object> conf = new HashMap<String, Object>();   
       conf.put(Config. TOPOLOGY_RECEIVER_BUFFER_SIZE , 8);  
       conf.put(Config. TOPOLOGY_TRANSFER_BUFFER_SIZE , 32);  
       conf.put(Config. TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE , 16384);  
       conf.put(Config. TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE , 16384);  
       //conf.put(Config.TOPOLOGY_DEBUG, true);
       
       LocalCluster cluster = new LocalCluster();  
       cluster.submitTopology(UVTopology. class .getSimpleName(), conf , builder .createTopology()); 
       
      /* try {
		StormSubmitter.submitTopology(UVTopology. class .getSimpleName(), conf, builder.createTopology());
	} catch (AlreadyAliveException e) {
		e.printStackTrace();
	} catch (InvalidTopologyException e) {
		e.printStackTrace();
	} catch (AuthorizationException e) {
		e.printStackTrace();
	}*/
       
	}
}
