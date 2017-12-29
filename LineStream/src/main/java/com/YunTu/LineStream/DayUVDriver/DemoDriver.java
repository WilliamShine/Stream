package com.YunTu.LineStream.DayUVDriver;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.YunTu.LineStream.DayUVDriver.DateFat.DateFmtBolt;
import com.YunTu.LineStream.DayUVDriver.DeepVisit.DeepVisitBolt;
import com.YunTu.LineStream.DayUVDriver.UPVsum.UPVSumBolt;
import com.YunTu.LineStream.mysqlUpdataBolt.JdbcUpdateBolt;
import com.google.common.collect.Maps;



public class DemoDriver {
	public static final String segmentation ="\001";
	
	private static final String KAFKA_SPOUT1_NAME = "kafka_spout1_name";
	private static final String DEFAULT_KAFKA_SPOUT1_NAME = "kafka_spout"+UUID.randomUUID();
	private static final String KAFKA_SPOUT1_parallelism_Number = "kafka_spout1_parallelism_number";
	private static final int DEFAULT_KAFKA_SPOUT1_parallelism_Number = 3;
	private static final String DateFmtBolt1_parallelism_Number = "datefmtbolt1_parallelism_number";
	private static final int DEFAULT_DateFmtBolt1_parallelism_Number = 3;
	
	private static final String KAFKA_SPOUT1_ZKCONN = "kafka_spout1_zkConn";
	private static final String KAFKA_SPOUT1_TOPICNAME = "kafka_spout1_topicname";
	private static final String KAFKA_SPOUT1_BROKERZKPATH = "kafka_spout1_brokerzkpath";
	private static final String KAFKA_SPOUT1_START_OFFSET_TIME = "kafka_spout1_start_offset_time";
	
	private static final String DEEPVISITBOLT_parallelism_Number = "deepvisitbolt_parallelism_number";
	private static final int DEFAULT_DEEPVISITBOLT_parallelism_Number = 3;
	private static final String DEEP_UPVSUMBOLT_parallelism_Number = "deep_upvsumbolt_parallelism_number";
	private static final int DEFAULT_DEEP_UPVSUMBOLT_parallelism_Number = 1;
	// JDBC连接 sqlupdata
	private static final String MYSQL_OUT_BOLT_NAME = "mysql_out_bolt_name";
	private static final String DEFAULT_MYSQL_OUT_BOLT_NAME = "mysql_out_bolt";
	private static final String MYSQL_WRITE_BOLT_parallelism_Number = "mysql_write_bolt_parallelism_number";
	private static final int DEFAULT_MYSQL_WRITE_BOLT_parallelism_Number = 1;
	
	private static final String JDBC_DATASOURCE_CLASSNAME = "dataSourceClassName";
	private static final String JDBC_DATASOURCE_URL = "dataSource.url";
	private static final String JDBC_DATASOURCE_USER = "dataSource.user";
	private static final String JDBC_DATASOURCE_PASSWORD = "dataSource.password";
	private static final String JDBC_TABLE_NAME = "table.name";
	private static final String JDBC_UPDATE_QUERY = "update.query";
	private static final String JDBC_QUERY_TIMEOUT_SECS = "query_timeout_secs";
	
	private static final String TOPOLOGY_WORKERS_Number = "topology_workers_number";
	private static final int DEFAULT_TOPOLOGY_WORKERS_Number = 4;
	private static final String TOPOLOGY_DEBUG_MODE = "topology_debug_mode";
	private static final boolean DEFAULT_TOPOLOGY_DEBUG_MODE = false;
	
	private static final String TOPOLOGY_MODE = "topology_mode";
	private static final String DEFAULT_TOPOLOGY_MODE = "local";

	public static void main(String[] args) {
		//配置文件读取配置
		if (args.length<1) {
			throw new RuntimeException("错误的参数个数："+args.length+"	请输入:配置文件地址");
		}
		String configPath=args[0];
		Properties prop = getProperties(configPath);
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(prop.getProperty(KAFKA_SPOUT1_NAME, DEFAULT_KAFKA_SPOUT1_NAME), kafakaSpoutUtil(prop), Integer.parseInt(prop.getProperty(KAFKA_SPOUT1_parallelism_Number, DEFAULT_KAFKA_SPOUT1_parallelism_Number+"")));
		builder.setBolt(DateFmtBolt.class.getSimpleName(), new DateFmtBolt(), Integer.parseInt(prop.getProperty(DateFmtBolt1_parallelism_Number, DEFAULT_DateFmtBolt1_parallelism_Number+"")))
			.shuffleGrouping(prop.getProperty(KAFKA_SPOUT1_NAME, DEFAULT_KAFKA_SPOUT1_NAME));
		builder.setBolt(DeepVisitBolt.class.getSimpleName(), new DeepVisitBolt(), Integer.parseInt(prop.getProperty(DEEPVISITBOLT_parallelism_Number, DEFAULT_DEEPVISITBOLT_parallelism_Number+"")))
			.fieldsGrouping(DateFmtBolt.class.getSimpleName(), new Fields("id"));
		builder.setBolt(UPVSumBolt.class.getSimpleName(), new UPVSumBolt(), Integer.parseInt(prop.getProperty(DEEP_UPVSUMBOLT_parallelism_Number, DEFAULT_DEEP_UPVSUMBOLT_parallelism_Number+"")))
			.shuffleGrouping(DeepVisitBolt.class.getSimpleName());
		
//		builder.setBolt(prop.getProperty(MYSQL_OUT_BOLT_NAME, DEFAULT_MYSQL_OUT_BOLT_NAME), MysqlupdateBoltUntil(prop), Integer.parseInt(prop.getProperty(MYSQL_WRITE_BOLT_parallelism_Number, DEFAULT_MYSQL_WRITE_BOLT_parallelism_Number+""))).shuffleGrouping(UPVSumBolt.class.getSimpleName());
		/*builder.setSpout("resourceSpout", new ResourceReadSpout(2000), 1);
		builder.setBolt("outBolt",  new MysqlChangerBolt(), 1).shuffleGrouping("resourceSpout");*/

		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_WORKERS, Integer.parseInt(prop.getProperty(TOPOLOGY_WORKERS_Number, DEFAULT_TOPOLOGY_WORKERS_Number+"")));
		conf.put(Config.TOPOLOGY_DEBUG, Boolean.parseBoolean(prop.getProperty(TOPOLOGY_DEBUG_MODE, DEFAULT_TOPOLOGY_DEBUG_MODE+"")));
		String switchStr = prop.getProperty(TOPOLOGY_MODE, DEFAULT_TOPOLOGY_MODE);
		switch (switchStr) {
		case "local":
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(DemoDriver.class.getSimpleName(), conf, builder.createTopology());
			break;
		case "online":
			try {
				StormSubmitter.submitTopology(DemoDriver.class.getSimpleName(), conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			} catch (AuthorizationException e) {
				e.printStackTrace();
			}
			break;
		default:
			throw new RuntimeException("错误的运行模式配置，配置local OR online");
		}
		 
	}
	
	/**
	 * 读取配置文件
	 * @param configPath
	 * @return
	 */
	private static Properties getProperties(String configPath) {
		Properties prop = new Properties();//配置具体容器
		try {
			//OriginalTopology.class.getClassLoader().getResourceAsStream(configPath)类加载器流在这不适用于读取外部配置
			BufferedReader bufferedReader = new BufferedReader(new FileReader(configPath));
			prop.load(bufferedReader);
			bufferedReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return prop;
	}
	
	/**
	 * 消费Kafka中的数据的spout。
	 * kafka数据推送要注意其\n
	 * @return
	 */
	private static KafkaSpout kafakaSpoutUtil(Properties prop) {
		//KafkaSpout
				String zkConnString =prop.getProperty(KAFKA_SPOUT1_ZKCONN);//！！！一定要配所有zk节点//vdata1:2181,vdata2:2181,vdata5:2181,vdata7:2181,vdata8:2181
				String topicName = prop.getProperty(KAFKA_SPOUT1_TOPICNAME);
				String brokerZkPath = prop.getProperty(KAFKA_SPOUT1_BROKERZKPATH);
				BrokerHosts hosts = new ZkHosts(zkConnString,brokerZkPath);// 这里的另一个重构方法，zookeeper中kafka的brokers的位置在哪个znode位置下，就需设置，（brokers可以指定位置的后续）
				SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());
				String statoff =prop.getProperty(KAFKA_SPOUT1_START_OFFSET_TIME);
				switch (statoff) {
				case "kafka.api.OffsetRequest.EarliestTime()":
					spoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();//Kafka从头消费
					break;
				case "kafka.api.OffsetRequest.LatestTime()":
					spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();//从结尾消费;
					break;
				case "System.currentTimeMillis()":
					spoutConfig.startOffsetTime = System.currentTimeMillis();//自纪元;
					break;
				default:
					throw new RuntimeException("Spout1错误的kafka消费起始方式");
				}
				spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());// 设置Scheme()！！！！从kafka到storm间的数据转化，可以自定义。可以仿StringScheme()自定义这个转换过程
				KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
				return kafkaSpout;
	}
	
	/**
	 * 数据更新,没有就创建。F
	 * @return
	 */
		private static JdbcUpdateBolt MysqlupdateBoltUntil(Properties prop) {
			Map<String, Object> hikariConfigMap = Maps.newHashMap();
			hikariConfigMap.put("dataSourceClassName", prop.getProperty(JDBC_DATASOURCE_CLASSNAME));
			hikariConfigMap.put("dataSource.url", prop.getProperty(JDBC_DATASOURCE_URL));
			hikariConfigMap.put("dataSource.user", prop.getProperty(JDBC_DATASOURCE_USER));
			hikariConfigMap.put("dataSource.password", prop.getProperty(JDBC_DATASOURCE_PASSWORD));
			ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

			String tableName = prop.getProperty(JDBC_TABLE_NAME);
			JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);

			JdbcUpdateBolt jdbcUpdateBolt = new JdbcUpdateBolt(connectionProvider, simpleJdbcMapper)
					.withInsertQuery(prop.getProperty(JDBC_UPDATE_QUERY))
					.withQueryTimeoutSecs(Integer.parseInt(prop.getProperty(JDBC_QUERY_TIMEOUT_SECS)));
			return jdbcUpdateBolt;

		} 
	
}
