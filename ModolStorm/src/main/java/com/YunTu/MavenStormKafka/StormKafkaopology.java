package com.YunTu.MavenStormKafka;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class StormKafkaopology {
	public static final String SPOUT_ID = KafkaSpout.class.getSimpleName();
	public static final String UVDEEPVISITBOLT_ID = SenqueceBolt.class.getSimpleName();

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(SPOUT_ID, kafakaSpoutUtil(), 2);
		builder.setBolt(UVDEEPVISITBOLT_ID, new SenqueceBolt(), 2).fieldsGrouping(SPOUT_ID, new Fields("id"));

		Map conf = new HashMap();
		conf.put(Config.TOPOLOGY_WORKERS, 4);
		// conf.put(Config. TOPOLOGY_RECEIVER_BUFFER_SIZE , 8);
		// conf.put(Config. TOPOLOGY_TRANSFER_BUFFER_SIZE , 32);
		// conf.put(Config. TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE , 16384);
		// conf.put(Config. TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE , 16384);
		// conf.put(Config.TOPOLOGY_DEBUG, true);

		// 本地运行
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(StormKafkaopology.class.getSimpleName(), conf, builder.createTopology());
		/*
		 * //集群模式 try { StormSubmitter.submitTopology(UVTopology. class
		 * .getSimpleName(), conf, builder.createTopology()); } catch
		 * (AlreadyAliveException e) { e.printStackTrace(); } catch
		 * (InvalidTopologyException e) { e.printStackTrace(); } catch
		 * (AuthorizationException e) { e.printStackTrace(); }
		 */
	}
	
	public static KafkaSpout kafakaSpoutUtil() {
		String zkConnString="J0:2181,J1:2181,J2:2181";
		BrokerHosts hosts = new ZkHosts(zkConnString);//这里的另一个重构方法，zookeeper中kafka的brokers的位置在哪个znode位置下，就需设置，（brokers可以指定位置的后续）
		SpoutConfig spoutConfig = new SpoutConfig(hosts, "sy1", "/sy1", UUID.randomUUID().toString());//zkroot随便设置
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());//设置Scheme()！！！！从kafka到storm间的数据转化，可以自定义。可以仿StringScheme()自定义这个转换过程
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);//kafkaSpout对象
		return kafkaSpout;
	}
}
