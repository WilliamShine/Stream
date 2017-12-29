package com.YunTu.Storm111.uvCount.KToKTD;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


public class KtoKTopologer {

	public static void main(String[] args) {
//KafkaSpout
		String zkConnString ="node01.cdh1:2181,node02.cdh1:2181,node03.cdh1:2181";//！！！一定要配所有zk节点//vdata1:2181,vdata2:2181,vdata5:2181,vdata7:2181,vdata8:2181
		String topicName = "sytest";
		String brokerZkPath = "/kafka/brokers";
		BrokerHosts hosts = new ZkHosts(zkConnString,brokerZkPath);
		SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());
		spoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();//Kafka从头消费
		//kafka.api.OffsetRequest.LatestTime()，从结尾消费；System.currentTimeMillis(),自纪元;
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		
//Topology
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("Kafka_spout", kafkaSpout, 5);
		/*builder.setBolt("3", new WordCountBolt1(), 3)
		         .fieldsGrouping("1", new Fields("word"));*/
		builder.setBolt("Simple_println", new GlobalCountBoltToK())
		         .globalGrouping("Kafka_spout");

//set Kafka producer properties.
		Properties props = new Properties();
		props.put("bootstrap.servers", "node01.cdh1:9092,node02.cdh1:9092,node03.cdh1:9092");
		props.put("acks", "1");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		KafkaBolt bolt = new KafkaBolt()
		        .withProducerProperties(props)
		        .withTopicSelector(new DefaultTopicSelector("sytest1"))
		        .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
		builder.setBolt("forwardToKafka", bolt, 8).shuffleGrouping("Simple_println");
		
		Map conf = new HashMap();
		conf.put(Config.TOPOLOGY_WORKERS, 4);
		//conf.put(Config.TOPOLOGY_DEBUG, true);

		/*LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("mytopology", conf, builder.createTopology());*/
		/*Utils.sleep(10000);
		cluster.shutdown();*/
		try {
			StormSubmitter.submitTopology("mytopology", conf, builder.createTopology());
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		} catch (AuthorizationException e) {
			e.printStackTrace();
		}
		

	}

}
