package com.YunTu.TopoLine.SelectDriver;

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
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
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

import com.YunTu.TopoLine.SelectDriver.bolt1.DateFmtBolt;
import com.YunTu.TopoLine.SelectDriver.bolt2.DateFmtBolt2;
import com.YunTu.TopoLine.SelectDriver.bolt3.DateFmtBolt3;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;




public class OriginalTopology {

	private static final String FMT_ID = DateFmtBolt.class.getSimpleName();
	private static final String FMT_ID2 = DateFmtBolt2.class.getSimpleName();
	private static final String FMT_ID3 = DateFmtBolt3.class.getSimpleName();
	private static final String DEEPVISITBOLT_ID = DeepVisitBolt.class.getSimpleName();
	
	public static final String segmentation ="\001";
	
	private static final String TOPOLOGY_MODE = "topology_mode";
	private static final String DEFAULT_TOPOLOGY_MODE = "local";
	
	//kafkaSpout1
	private static final String KAFKA_SPOUT1_NAME = "kafka_spout1_name";
	private static final String DEFAULT_KAFKA_SPOUT1_NAME = "kafka_spout1";
	private static final String KAFKA_SPOUT1_parallelism_Number = "kafka_spout1_parallelism_number";
	private static final int DEFAULT_KAFKA_SPOUT1_parallelism_Number = 3;
	private static final String DateFmtBolt1_parallelism_Number = "datefmtbolt1_parallelism_number";
	private static final int DEFAULT_DateFmtBolt1_parallelism_Number = 3;
	private static final String KAFKA_SPOUT1_ZKCONN = "kafka_spout1_zkConn";
	private static final String KAFKA_SPOUT1_TOPICNAME = "kafka_spout1_topicname";
	private static final String KAFKA_SPOUT1_BROKERZKPATH = "kafka_spout1_brokerzkpath";
	private static final String KAFKA_SPOUT1_START_OFFSET_TIME = "kafka_spout1_start_offset_time";
	public static final String  T_EBIZ_COMMENT_RELEASE_DATE = "t_ebiz_comment.release_date";//35
	public static final String  T_EBIZ_COMMENT_HIT_TAG = "t_ebiz_comment.hit_tag";//33
	public static final String  DSSLRESULT_DWTVS_TYPE = "DSSLresult.dwtvs_type";///eb/
	
	//kafkaSpout2
	private static final String KAFKA_SPOUT2_NAME = "kafka_spout2_name";
	private static final String DEFAULT_KAFKA_SPOUT2_NAME = "kafka_spout2";
	private static final String KAFKA_SPOUT2_parallelism_Number = "kafka_spout2_parallelism_number";
	private static final int DEFAULT_KAFKA_SPOUT2_parallelism_Number = 3;
	private static final String DateFmtBolt2_parallelism_Number = "datefmtbolt2_parallelism_number";
	private static final int DEFAULT_DateFmtBolt2_parallelism_Number = 3;
	private static final String KAFKA_SPOUT2_ZKCONN = "kafka_spout2_zkConn";
	private static final String KAFKA_SPOUT2_TOPICNAME = "kafka_spout2_topicname";
	private static final String KAFKA_SPOUT2_BROKERZKPATH = "kafka_spout2_brokerzkpath";
	private static final String KAFKA_SPOUT2_START_OFFSET_TIME = "kafka_spout2_start_offset_time";
	public static final String  T_DATA_RELEASE_DATE = "t_data.release_date";//1
	public static final String  T_DATA_URL_CRC = "t_data.url_crc";//0
	public static final String  T_DATA_REL_TYPE = "t_data.rel_type";//2
	public static final String  T_LABEL_URL_CRC = "t_label.url_crc";//3
	public static final String  T_LABEL_HIT_TAG = "t_label.hit_tag";//5
	public static final String  FDSZTSL_RESULT_DWTVS_TYPE = "fdsztsl_result.dwtvs_type";///neb/theme_volume/
	public static final String  FDSZTSL_T_LABEL_HDFSIP = "fdsztsl_t_label_hdfsip";//hdfs://node01.cdh1:8020
	public static final String  FDSZTSL_T_LABEL_PATH = "fdsztsl_T_label_path";///user/shine/1407714309205630_76131_10214/t_label
	
	//kafkaSpout3
	private static final String KAFKA_SPOUT3_NAME = "kafka_spout3_name";
	private static final String DEFAULT_KAFKA_SPOUT3_NAME = "kafka_spout3";
	private static final String KAFKA_SPOUT3_parallelism_Number = "kafka_spout3_parallelism_number";
	private static final int DEFAULT_KAFKA_SPOUT3_parallelism_Number = 3;
	private static final String DateFmtBolt3_parallelism_Number = "datefmtbolt3_parallelism_number";
	private static final int DEFAULT_DateFmtBolt3_parallelism_Number = 3;
	private static final String KAFKA_SPOUT3_ZKCONN = "kafka_spout3_zkConn";
	private static final String KAFKA_SPOUT3_TOPICNAME = "kafka_spout3_topicname";
	private static final String KAFKA_SPOUT3_BROKERZKPATH = "kafka_spout3_brokerzkpath";
	private static final String KAFKA_SPOUT3_START_OFFSET_TIME = "kafka_spout3_start_offset_time";
	public static final String  T_COMMENT_RELEASE_DATE = "t_comment.release_date";//5
	public static final String  T_COMMENT_C_URL_CRC = "t_comment.c_url_crc";//44
	public static final String  DT3_T_DATA_URL_CRC = "dt3_t_data.url_crc";//0
	public static final String  DT3_T_DATA_REL_TYPE = "dt3_t_data.rel_type";//2
	public static final String  DT3_T_LABEL_URL_CRC = "dt3_t_label.url_crc";//3
	public static final String  DT3_T_LABEL_HIT_TAG = "dt3_t_label.hit_tag";//5
	public static final String  FDSPLSLRESULT_DWTVS_TYPE = "fdsplsl_result.dwtvs_type";///neb/comment_volume/
	public static final String  FDSPLSL_T_DATA_HDFSIP = "fdsplsl_t_data_hdfsip";//hdfs://node01.cdh1:8020/
	public static final String  FDSPLSL_T_DATA_PATH = "fdsplsl_t_data_path";///user/shine/1407714309205630_76131_10214/t_data_3
	public static final String  FDSPLSL_T_LABEL_HDFSIP = "fdsplsl_t_label_hdfsip";//hdfs://node01.cdh1:8020
	public static final String  FDSPLSL_T_LABEL_PATH = "fdsplsl_T_label_path";///user/shine/1407714309205630_76131_10214/t_label
	
	public static final String  TOPOLOGY_WORK_JOSN = "topology_work_josn";
	
	private static final String DEEPVISITBOLT_parallelism_Number = "deepvisitbolt_parallelism_number";
	private static final int DEFAULT_DEEPVISITBOLT_parallelism_Number = 3;
	
	//conf
	private static final String TOPOLOGY_WORKERS_Number = "topology_workers_number";
	private static final int DEFAULT_TOPOLOGY_WORKERS_Number = 4;
	public static final String DEEPBOLT_INTERVAL_TIME ="deepbolt_interval_time";
	private static final long DEFAULT_DEEPBOLT_INTERVAL_TIME =20;
	public static final String DEEPBOLT_INTERVAL_NUM ="deepbolt_interval_num";
	private static final long DEFAULT_DEEPBOLT_INTERVAL_NUM =100;
	
	//JDBC连接	sqlupdata
	private static final String MYSQL_OUT_BOLT_NAME = "mysql_out_bolt_name";
	private static final String DEFAULT_MYSQL_OUT_BOLT_NAME = "mysql_out_bolt";
	private static final String MYSQL_WRITE_BOLT_parallelism_Number = "mysql_write_bolt_parallelism_number";
	private static final int DEFAULT_MYSQL_WRITE_BOLT_parallelism_Number = 3;
	
	private static final String JDBC_DATASOURCE_CLASSNAME = "dataSourceClassName";
	private static final String JDBC_DATASOURCE_URL = "dataSource.url";
	private static final String JDBC_DATASOURCE_USER = "dataSource.user";
	private static final String JDBC_DATASOURCE_PASSWORD = "dataSource.password";
	private static final String JDBC_TABLE_NAME = "table.name";
	private static final String JDBC_UPDATE_QUERY = "update.query";
	private static final String JDBC_QUERY_TIMEOUT_SECS = "query_timeout_secs";
	
	
	public static void main(String[] args) {
		//读取参数
		if (args.length<1) {
			throw new RuntimeException("错误的参数个数："+args.length+"	输入：类加载器getResourceAsStream配置文件地址");
		}
		String configPath=args[0];
		Properties prop = getProperties(configPath);
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(prop.getProperty(KAFKA_SPOUT1_NAME, DEFAULT_KAFKA_SPOUT1_NAME), kafakaSpoutUtil(prop), Integer.parseInt(prop.getProperty(KAFKA_SPOUT1_parallelism_Number, DEFAULT_KAFKA_SPOUT1_parallelism_Number+"")));
		builder.setBolt(FMT_ID, new DateFmtBolt("DC001_/eb/"), Integer.parseInt(prop.getProperty(DateFmtBolt1_parallelism_Number, DEFAULT_DateFmtBolt1_parallelism_Number+""))).shuffleGrouping(prop.getProperty(KAFKA_SPOUT1_NAME, DEFAULT_KAFKA_SPOUT1_NAME));
		
		builder.setSpout(prop.getProperty(KAFKA_SPOUT2_NAME, DEFAULT_KAFKA_SPOUT2_NAME), kafakaSpoutUtil2(prop), Integer.parseInt(prop.getProperty(KAFKA_SPOUT2_parallelism_Number, DEFAULT_KAFKA_SPOUT2_parallelism_Number+"")));
		builder.setBolt(FMT_ID2, new DateFmtBolt2(), Integer.parseInt(prop.getProperty(DateFmtBolt2_parallelism_Number, DEFAULT_DateFmtBolt2_parallelism_Number+""))).shuffleGrouping(prop.getProperty(KAFKA_SPOUT2_NAME, DEFAULT_KAFKA_SPOUT2_NAME));
		
		builder.setSpout(prop.getProperty(KAFKA_SPOUT3_NAME, DEFAULT_KAFKA_SPOUT3_NAME), kafakaSpoutUtil3(prop), Integer.parseInt(prop.getProperty(KAFKA_SPOUT3_parallelism_Number, DEFAULT_KAFKA_SPOUT3_parallelism_Number+"")));
		builder.setBolt(FMT_ID3, new DateFmtBolt3(), Integer.parseInt(prop.getProperty(DateFmtBolt3_parallelism_Number, DEFAULT_DateFmtBolt3_parallelism_Number+""))).shuffleGrouping(prop.getProperty(KAFKA_SPOUT3_NAME, DEFAULT_KAFKA_SPOUT3_NAME));

		
		builder.setBolt(DEEPVISITBOLT_ID, new DeepVisitBolt(), Integer.parseInt(prop.getProperty(DEEPVISITBOLT_parallelism_Number, DEFAULT_DEEPVISITBOLT_parallelism_Number+"")))
			.fieldsGrouping(FMT_ID, new Fields("id"))
			.fieldsGrouping(FMT_ID2, new Fields("id"))
			.fieldsGrouping(FMT_ID3, new Fields("id"));
		builder.setBolt(prop.getProperty(MYSQL_OUT_BOLT_NAME, DEFAULT_MYSQL_OUT_BOLT_NAME), MysqlupdateBoltUntil(prop), Integer.parseInt(prop.getProperty(MYSQL_WRITE_BOLT_parallelism_Number, DEFAULT_MYSQL_WRITE_BOLT_parallelism_Number+""))).shuffleGrouping(DEEPVISITBOLT_ID);

		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_WORKERS, Integer.parseInt(prop.getProperty(TOPOLOGY_WORKERS_Number, DEFAULT_TOPOLOGY_WORKERS_Number+"")));
//		conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
		/*conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
		conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
		conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);*/
//		 conf.put(Config.TOPOLOGY_DEBUG, true);
		conf.put(DEEPBOLT_INTERVAL_TIME, Long.parseLong(prop.getProperty(DEEPBOLT_INTERVAL_TIME, DEFAULT_DEEPBOLT_INTERVAL_TIME+"")));
		conf.put(DEEPBOLT_INTERVAL_NUM, Long.parseLong(prop.getProperty(DEEPBOLT_INTERVAL_NUM, DEFAULT_DEEPBOLT_INTERVAL_NUM+"")));
		conf.put( T_EBIZ_COMMENT_RELEASE_DATE ,prop.getProperty(T_EBIZ_COMMENT_RELEASE_DATE));
		conf.put( T_EBIZ_COMMENT_HIT_TAG ,prop.getProperty(T_EBIZ_COMMENT_HIT_TAG));
		conf.put( DSSLRESULT_DWTVS_TYPE ,prop.getProperty(DSSLRESULT_DWTVS_TYPE));

		conf.put(T_DATA_RELEASE_DATE,prop.getProperty(T_DATA_RELEASE_DATE));
		conf.put(T_DATA_URL_CRC,prop.getProperty(T_DATA_URL_CRC));
		conf.put(T_DATA_REL_TYPE,prop.getProperty(T_DATA_REL_TYPE));
		conf.put(T_LABEL_URL_CRC,prop.getProperty(T_LABEL_URL_CRC));
		conf.put(T_LABEL_HIT_TAG,prop.getProperty(T_LABEL_HIT_TAG));
		conf.put(FDSZTSL_RESULT_DWTVS_TYPE,prop.getProperty(FDSZTSL_RESULT_DWTVS_TYPE));
		conf.put(FDSZTSL_T_LABEL_HDFSIP,prop.getProperty(FDSZTSL_T_LABEL_HDFSIP));
		conf.put(FDSZTSL_T_LABEL_PATH,prop.getProperty(FDSZTSL_T_LABEL_PATH));

		conf.put(T_COMMENT_RELEASE_DATE,prop.getProperty(T_COMMENT_RELEASE_DATE));
		conf.put(T_COMMENT_C_URL_CRC,prop.getProperty(T_COMMENT_C_URL_CRC));
		conf.put(DT3_T_DATA_URL_CRC,prop.getProperty(DT3_T_DATA_URL_CRC));
		conf.put(DT3_T_DATA_REL_TYPE,prop.getProperty(DT3_T_DATA_REL_TYPE));
		conf.put(DT3_T_LABEL_URL_CRC,prop.getProperty(DT3_T_LABEL_URL_CRC));
		conf.put(DT3_T_LABEL_HIT_TAG,prop.getProperty(DT3_T_LABEL_HIT_TAG));
		conf.put(FDSPLSLRESULT_DWTVS_TYPE,prop.getProperty(FDSPLSLRESULT_DWTVS_TYPE));
		conf.put(FDSPLSL_T_DATA_HDFSIP,prop.getProperty(FDSPLSL_T_DATA_HDFSIP));
		conf.put(FDSPLSL_T_DATA_PATH,prop.getProperty(FDSPLSL_T_DATA_PATH));
		conf.put(FDSPLSL_T_LABEL_HDFSIP,prop.getProperty(FDSPLSL_T_LABEL_HDFSIP));
		conf.put(FDSPLSL_T_LABEL_PATH,prop.getProperty(FDSPLSL_T_LABEL_PATH));
		conf.put(TOPOLOGY_WORK_JOSN,prop.getProperty("json"));
		
		
		
		switch (prop.getProperty(TOPOLOGY_MODE, DEFAULT_TOPOLOGY_MODE)) {
		case "local":
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(OriginalTopology.class.getSimpleName(), conf, builder.createTopology());
			break;
		case "online":
			try {
				StormSubmitter.submitTopology(OriginalTopology.class.getSimpleName(), conf, builder.createTopology());
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
	 * 
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
//					break;
				}
				spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());// 设置Scheme()！！！！从kafka到storm间的数据转化，可以自定义。可以仿StringScheme()自定义这个转换过程
				KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
				return kafkaSpout;
	}
	
	private static KafkaSpout kafakaSpoutUtil2(Properties prop) {
		//KafkaSpout
				String zkConnString =prop.getProperty(KAFKA_SPOUT2_ZKCONN);//！！！一定要配所有zk节点//vdata1:2181,vdata2:2181,vdata5:2181,vdata7:2181,vdata8:2181
				String topicName = prop.getProperty(KAFKA_SPOUT2_TOPICNAME);
				String brokerZkPath = prop.getProperty(KAFKA_SPOUT2_BROKERZKPATH);
				BrokerHosts hosts = new ZkHosts(zkConnString,brokerZkPath);// 这里的另一个重构方法，zookeeper中kafka的brokers的位置在哪个znode位置下，就需设置，（brokers可以指定位置的后续）
				SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());
				String statoff =prop.getProperty(KAFKA_SPOUT2_START_OFFSET_TIME);
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
					throw new RuntimeException("Spout2错误的kafka消费起始方式");
//					break;
				}
				spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());// 设置Scheme()！！！！从kafka到storm间的数据转化，可以自定义。可以仿StringScheme()自定义这个转换过程
				KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
				return kafkaSpout;
	}
	
	private static KafkaSpout kafakaSpoutUtil3(Properties prop) {
		//KafkaSpout
				String zkConnString =prop.getProperty(KAFKA_SPOUT3_ZKCONN);//！！！一定要配所有zk节点//vdata1:2181,vdata2:2181,vdata5:2181,vdata7:2181,vdata8:2181
				String topicName = prop.getProperty(KAFKA_SPOUT3_TOPICNAME);
				String brokerZkPath = prop.getProperty(KAFKA_SPOUT3_BROKERZKPATH);
				BrokerHosts hosts = new ZkHosts(zkConnString,brokerZkPath);// 这里的另一个重构方法，zookeeper中kafka的brokers的位置在哪个znode位置下，就需设置，（brokers可以指定位置的后续）
				SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());
				String statoff =prop.getProperty(KAFKA_SPOUT3_START_OFFSET_TIME);
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
					throw new RuntimeException("Spout3错误的kafka消费起始方式");
//					break;
				}
				spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());// 设置Scheme()！！！！从kafka到storm间的数据转化，可以自定义。可以仿StringScheme()自定义这个转换过程
				KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
				return kafkaSpout;
	}
	
	/**
	 * 向mysql表中插入数据
	 * 
	 * @return
	 */
	private static JdbcInsertBolt MysqlInsertBoltUntil() {
		
/*// 自写简单连接池实现
		
		 //创建一个connectionProvider 
		MysqlConnectionProvider conectionProvider = new MysqlConnectionProvider();
		 
		 //创建一个mapper，填写表名 
		JdbcMapper mapper = new SimpleJdbcMapper("sy_count",conectionProvider);
		 
		 //通过mapper创建一个bolt组件
		  
		  return new JdbcInsertBolt(conectionProvider,mapper)
		  .withTableName("sy_count") .withQueryTimeoutSecs(30);*/
		 

// HikariCP实现的连接池
		Map<String, Object> hikariConfigMap = Maps.newHashMap();
		hikariConfigMap.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
		hikariConfigMap.put("dataSource.url", "jdbc:mysql://47.100.9.7:3306/data_warehouse");
		hikariConfigMap.put("dataSource.user", "weiwei.wu");
		hikariConfigMap.put("dataSource.password", "Miweiwei20170711@");
		ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

		String tableName = "DTstorm_result";
		// 初始化映射器
		JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);
/*// 添加所有字段
		
		 List<Column> columnSchema = Lists.newArrayList( new Column("user_id",java.sql.Types.INTEGER), new Column("user_name", java.sql.Types.VARCHAR));
		 JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(columnSchema);
		 JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider,simpleJdbcMapper) .withTableName(tableName) .withQueryTimeoutSecs(30);*/
		 
// 非添加所有字段
		JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
				.withInsertQuery("insert into DTstorm_result(dwtvs_task,dwtvs_logo,dwtvs_name,dwtvs_date_year,dwtvs_date_month,dwtvs_date_day,dwtvs_date_hour,dwtvs_count_num,dwtvs_type) values (?,?,?,?,?,?,?,?,?)")// insert into sy_count(id,UV,PV)
																					// values (?,?,?)//update sy_count
																					// set UV=?,PV=? where id=?
				.withQueryTimeoutSecs(30);
		return userPersistanceBolt;

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
