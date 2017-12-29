package com.YunTu.MStorm.simpleKafka;

import java.util.UUID;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
/**
 * KafkaSpout
 * 因为bolt有很大的不合常规，单独拿出
 * KafkaSpout提供了带参数的构造方法
 * KafkaBolt没有提供带参数的构造方法，交给了上一级去设置参数等过程，要是改一下源码就方便多了
 * @author 84031
 *
 */
public class KafakaSpoutTopology {

	/**
	 * 作为一个kafka消费者获得数据，用bolt输出到控制台
	 * @param args
	 * 
	 */
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("kafkaSpout", kafakaSpoutUtil(), 3);
									//3个线程，需要注意kfaka当中设置了几个分区，一般相等，太多就倍数，3个线程去读kafka的3个分区
		builder.setBolt("outBolt", new OutClientBolt(), 3).shuffleGrouping("kafkaSpout");//简单输出
		Config conf=new Config();
		conf.setNumWorkers(4);
		conf.put(Config.TOPOLOGY_DEBUG, false);

		//在本地运行的方式
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("KafkaTopology", conf, builder.createTopology());

		//服务器运行
//		cluster.submitTopology("mytopology", conf, builder.createTopology());
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

}
