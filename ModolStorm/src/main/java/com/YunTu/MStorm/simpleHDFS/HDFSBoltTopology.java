package com.YunTu.MStorm.simpleHDFS;


import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

/**
 * 未测
 * Kafka-HDFS
 * @author 84031
 *
 */
public class HDFSBoltTopology {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		System.setProperty("hadoop.home.dir", "F:\\hadoop-2.6.0-cdh5.5.0");
		System.setProperty("HADOOP_USER_NAME", "root");
		
		builder.setSpout("1号Spout", kafakaSpoutUtil(), 2);
		builder.setBolt("3号bolt", HDFSBoltUntil(), 3).shuffleGrouping("1号Spout");

		Map conf = new HashMap();
		conf.put(Config.TOPOLOGY_WORKERS, 4);
		conf.put(Config.TOPOLOGY_DEBUG, false);

		//本地运行
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Kafka_HDFS_topology", conf, builder.createTopology());
		
		/*Utils.sleep(10000);
		cluster.shutdown();*/

		//StormSubmitter.submitTopology("mytopology", conf, builder.createTopology());
	}
	/**
	 * kafka输入数据Spout
	 * @return
	 */
	private static KafkaSpout kafakaSpoutUtil() {
		String zkConnString="J0:2181,J1:2181,J2:2181";
		BrokerHosts hosts = new ZkHosts(zkConnString);//这里的另一个重构方法，zookeeper中kafka的brokers的位置在哪个znode位置下，就需设置，（brokers可以指定位置的后续）
		SpoutConfig spoutConfig = new SpoutConfig(hosts, "sy1", "/sy1", UUID.randomUUID().toString());//zkroot随便设置
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());//设置Scheme()！！！！从kafka到storm间的数据转化，可以自定义。可以仿StringScheme()自定义这个转换过程
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);//kafkaSpout对象
		return kafkaSpout;
	}
	/**
	 * HDFS输出数据Bolt
	 * @return
	 */
	private static HdfsBolt HDFSBoltUntil() {
		// use "|" instead of "," for field delimiter
		RecordFormat format = new DelimitedRecordFormat()
		        .withFieldDelimiter("|");

		// sync the filesystem after every 1k tuples
		SyncPolicy syncPolicy = new CountSyncPolicy(10);

		// rotate files when they reach 5MB
		FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);

		FileNameFormat fileNameFormat = new DefaultFileNameFormat()
		        .withPath("/ShangYue/day27/").withPrefix("前缀");

		HdfsBolt bolt = new HdfsBolt()
		        .withFsUrl("hdfs://J0:9000")
		        .withFileNameFormat(fileNameFormat)
		        .withRecordFormat(format)
		        .withRotationPolicy(rotationPolicy)
		        .withSyncPolicy(syncPolicy);
		return bolt;
	}

}
