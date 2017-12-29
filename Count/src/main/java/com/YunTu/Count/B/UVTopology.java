package com.YunTu.Count.B;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


public class UVTopology {

//	public static final String SPOUT_ID = SourceSpout.class .getSimpleName();  
    public static final String UVFMT_ID = UVFmtBolt.class .getSimpleName();  
    public static final String UVDEEPVISITBOLT_ID = UVDeepVisitBolt.class .getSimpleName();  
    public static final String UVSUMBOLT_ID = UVSumBolt.class .getSimpleName();  
    
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();  
        builder.setSpout( "kafakaSpoutUtil" , kafakaSpoutUtil(), 1);  
        // 切分格式化数据源： 格式： 2017_05_13686666420349405689
        builder.setBolt( UVFMT_ID , new UVFmtBolt(), 4).shuffleGrouping("kafakaSpoutUtil");  
        // 统计每个线程 对应的 UV ,格式： 20151010_ABYH6Y4V4SCV 4  
        builder.setBolt( UVDEEPVISITBOLT_ID , new UVDeepVisitBolt(), 4).fieldsGrouping( UVFMT_ID,  
                new Fields("id"));  
        // 单线程汇总  
        builder.setBolt( UVSUMBOLT_ID , new UVSumBolt(), 1).shuffleGrouping(UVDEEPVISITBOLT_ID ); 
        //mysql 输出
        //builder.setBolt( "mysql_JDBCBolt" , MysqlBoltUntil(), 1).shuffleGrouping(UVSUMBOLT_ID ); 

       
       Map<String, Object> conf = new HashMap<String, Object>();   
       //conf.put(Config. TOPOLOGY_RECEIVER_BUFFER_SIZE , 8);  
       conf.put(Config. TOPOLOGY_TRANSFER_BUFFER_SIZE , 32);  
       conf.put(Config. TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE , 16384);  
       conf.put(Config. TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE , 16384);  
       //conf.put(Config.TOPOLOGY_DEBUG, true);
       
       LocalCluster cluster = new LocalCluster();  
       cluster.submitTopology(UVTopology. class .getSimpleName(), conf , builder .createTopology()); 
       
	}
	
	/**
	 * kafkaspout产生方法，本身相当于一个kafka的消费者
	 */
	private static KafkaSpout kafakaSpoutUtil() {
		String zkConnString="47.100.9.7:2181";
		BrokerHosts hosts = new ZkHosts(zkConnString);//这里的另一个重构方法，zookeeper中kafka的brokers的位置在哪个znode位置下，就需设置，（brokers可以指定位置的后续）
		SpoutConfig spoutConfig = new SpoutConfig(hosts, "sytest", "/shine", UUID.randomUUID().toString());//zkroot随便设置
		spoutConfig.scheme = new SchemeAsMultiScheme(new KafkaScheme());//设置Scheme()！！！！从kafka到storm间的数据转化，可以自定义。可以仿StringScheme()自定义这个转换过程
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);//kafkaSpout对象
		return kafkaSpout;
	}
	
	/*private static JdbcInsertBolt MysqlBoltUntil() {
		//创建一个connectionProvider
		MysqlConnectionProvider conectionProvider = new MysqlConnectionProvider();
        
        //创建一个mapper，填写表名
        JdbcMapper mapper = new SimpleJdbcMapper("result",conectionProvider);
        
        //通过mapper创建一个bolt组件
        
        return new JdbcInsertBolt(conectionProvider,mapper)
                .withTableName("result")
                .withQueryTimeoutSecs(30);

	}*/
}
