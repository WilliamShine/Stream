package com.YunTu.Storm111.uvCount.FFF;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class TopologerShine111 {
	
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("1", new WordSpout1(true), 5);
		builder.setSpout("2", new WordSpout1(true), 3);
		builder.setBolt("3", new WordCountBolt1(),3)//
		         .fieldsGrouping("1", new Fields("word"))
		         .fieldsGrouping("2", new Fields("word"));
		/*builder.setBolt("4", new GlobalCountBolt1())
		         .globalGrouping("1");*/

		Map conf = new HashMap();
		conf.put(Config.TOPOLOGY_WORKERS, 4);
		//conf.put(Config.TOPOLOGY_DEBUG, true);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("mytopology", conf, builder.createTopology());
		/*Utils.sleep(10000);
		cluster.shutdown();*/
		/*try {
			StormSubmitter.submitTopology("mytopology", conf, builder.createTopology());
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		} catch (AuthorizationException e) {
			e.printStackTrace();
		}*/
	}

}
