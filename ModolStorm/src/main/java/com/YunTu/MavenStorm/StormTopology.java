package com.YunTu.MavenStorm;

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

public class StormTopology {
	public static final String SPOUT_ID = NumberSpout.class.getSimpleName();
	public static final String UVDEEPVISITBOLT_ID = SumBolt.class.getSimpleName();

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(SPOUT_ID, new NumberSpout(), 2);
		builder.setBolt(UVDEEPVISITBOLT_ID, new SumBolt(), 2).fieldsGrouping(SPOUT_ID, new Fields("id"));

		Map conf = new HashMap();
		conf.put(Config.TOPOLOGY_WORKERS, 4);
		// conf.put(Config. TOPOLOGY_RECEIVER_BUFFER_SIZE , 8);
		// conf.put(Config. TOPOLOGY_TRANSFER_BUFFER_SIZE , 32);
		// conf.put(Config. TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE , 16384);
		// conf.put(Config. TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE , 16384);
		// conf.put(Config.TOPOLOGY_DEBUG, true);

		// 本地运行
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(StormTopology.class.getSimpleName(), conf, builder.createTopology());
		/*
		 * //集群模式 try { StormSubmitter.submitTopology(UVTopology. class
		 * .getSimpleName(), conf, builder.createTopology()); } catch
		 * (AlreadyAliveException e) { e.printStackTrace(); } catch
		 * (InvalidTopologyException e) { e.printStackTrace(); } catch
		 * (AuthorizationException e) { e.printStackTrace(); }
		 */
	}
}
