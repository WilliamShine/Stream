package com.YunTu.MStorm.simpleCount;

import java.util.HashMap;
import java.util.Map;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class MyTopologyDriver {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("1号Spout", new MySpout(), 2);
		builder.setBolt("3号bolt", new MyBolt(), 2).fieldsGrouping("1号Spout", new Fields("1号Spout标识"));

		Map conf = new HashMap();
		conf.put(Config.TOPOLOGY_WORKERS, 4);
		conf.put(Config.TOPOLOGY_DEBUG, false);

		//本地运行
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("mytopology2", conf, builder.createTopology());
		Utils.sleep(10000);
		cluster.shutdown();

		//StormSubmitter.submitTopology("mytopology", conf, builder.createTopology());
	}

}
