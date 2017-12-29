package com.YunTu.MStorm.simpleKafka;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
/**
 * 因为bolt有很大的不合常规，单独拿出
 * KafkaSpout提供了带参数的构造方法
 * KafkaBolt没有提供带参数的构造方法，交给了上一级去设置参数等过程，要是改一下源码就方便多了
 * @author 84031
 *
 */
public class KafakaBoltTopology {
	/**
	 * 作为生产者向kafka中打入数据,从sy1，经过解析bolt，有发送到另一个主题sy2中
	 */
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		
		Config conf=new Config();
		conf.setNumWorkers(4);
		conf.put(Config.TOPOLOGY_DEBUG, false);
		
		KafkaBoltuntil(conf);//KafkaBolt只需要在conf当中设置一些参数就可以

		builder.setSpout("kafkaSpout", kafakaSpoutUtil2(), 3);
		builder.setBolt("parserBolt", new OutClientBolt(),3)
				.shuffleGrouping("kafkaSpout");// 简单输出，在这里，拓扑里，要么注释掉，要么就用上
		builder.setBolt("tokafkaBolt", new KafkaBolt<String,String>(),3)
				.shuffleGrouping("parserBolt");

		//在本地运行的方式
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("KafkaTopology5", conf, builder.createTopology());
		
//		StormSubmitter.submitTopology("mytopology", conf, builder.createTopology());
	}
	
	/**
	 * kafkaspout消费消息方法，本身相当于一个kafka的消费者
	 */
	private static KafkaSpout kafakaSpoutUtil2() {
		String zkConnString="47.100.9.7:2181";
		BrokerHosts hosts = new ZkHosts(zkConnString);//这里的另一个重构方法，zookeeper中kafka的brokers的位置在哪个znode位置下，就需设置，（brokers可以指定位置的后续）
		SpoutConfig spoutConfig = new SpoutConfig(hosts, "sytest", "/shine", UUID.randomUUID().toString());//zkroot随便设置
		spoutConfig.scheme = new SchemeAsMultiScheme(new KafkaScheme());//设置Scheme()！！！！从kafka到storm间的数据转化，可以自定义。可以仿StringScheme()自定义这个转换过程
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);//kafkaSpout对象
		return kafkaSpout;
	}
	
	/**
	 * 消息生产方法
	 * @param conf
	 */
	private static void KafkaBoltuntil(Config conf) {
		// bolt独有的配置
		Map<String, String> map = new HashMap<String, String>();
		//  配置Kafka broker地址
		map.put("metadata.broker.list", "47.100.9.7:9092");
		//  serializer.class为消息的序列化类
		map.put("serializer.class", "kafka.serializer.StringEncoder");
		conf.put("kafka.broker.properties", map);
		//  配置KafkaBolt生成的topic
		conf.put("topic", "test1");
	}
}
