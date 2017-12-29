package com.YunTu.uvCount.C;

import java.util.HashMap;
import java.util.Map;
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
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.google.common.collect.Maps;



public class UVTopology {

	// public static final String SPOUT_ID = SourceSpout.class .getSimpleName();
	public static final String UVFMT_ID = UVFmtBolt.class.getSimpleName();
	public static final String UVDEEPVISITBOLT_ID = UVDeepVisitBolt.class.getSimpleName();
	public static final String UVSUMBOLT_ID = UVSumBolt.class.getSimpleName();

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafakaSpoutUtil", kafakaSpoutUtil(), 1);
		builder.setBolt(UVFMT_ID, new UVFmtBolt(), 4).shuffleGrouping("kafakaSpoutUtil");
		builder.setBolt(UVDEEPVISITBOLT_ID, new UVDeepVisitBolt(), 4).fieldsGrouping(UVFMT_ID, new Fields("id"));
		builder.setBolt(UVSUMBOLT_ID, new UVSumBolt(), 1).shuffleGrouping(UVDEEPVISITBOLT_ID);
		builder.setBolt("mysql_JDBCBolt", MysqlupdateBoltUntil(), 1).shuffleGrouping(UVSUMBOLT_ID);

		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_WORKERS, 4);
		/*conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
		conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
		conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
		conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);*/
		// conf.put(Config.TOPOLOGY_DEBUG, true);

		
		  LocalCluster cluster = new LocalCluster(); cluster.submitTopology(UVTopology.
		  class .getSimpleName(), conf , builder .createTopology());
		 
			/*try {
				StormSubmitter.submitTopology(UVTopology.class.getSimpleName(), conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			} catch (AuthorizationException e) {
				e.printStackTrace();
			}*/
		
	}

	/**
	 * 消费Kafka中的数据的spout。
	 * 
	 * @return
	 */
	private static KafkaSpout kafakaSpoutUtil() {
		String zkConnString = "47.100.9.7:2181";
		BrokerHosts hosts = new ZkHosts(zkConnString);// 这里的另一个重构方法，zookeeper中kafka的brokers的位置在哪个znode位置下，就需设置，（brokers可以指定位置的后续）
		SpoutConfig spoutConfig = new SpoutConfig(hosts, "sytest", "/shine", UUID.randomUUID().toString());// zkroot随便设置
		spoutConfig.scheme = new SchemeAsMultiScheme( new StringScheme());// 设置Scheme()！！！！从kafka到storm间的数据转化，可以自定义。可以仿StringScheme()自定义这个转换过程
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);// kafkaSpout对象
		return kafkaSpout;
	}

	/**
	 * 向mysql表中插入数据
	 * 
	 * @return
	 */
	private static JdbcInsertBolt MysqlInsertBoltUntil() {
		// 自写简单连接池实现
		/*
		 * //创建一个connectionProvider MysqlConnectionProvider conectionProvider = new
		 * MysqlConnectionProvider();
		 * 
		 * //创建一个mapper，填写表名 JdbcMapper mapper = new
		 * SimpleJdbcMapper("sy_count",conectionProvider);
		 * 
		 * //通过mapper创建一个bolt组件
		 * 
		 * return new JdbcInsertBolt(conectionProvider,mapper)
		 * .withTableName("sy_count") .withQueryTimeoutSecs(30);
		 */

		// HikariCP实现的连接池
		Map<String, Object> hikariConfigMap = Maps.newHashMap();
		hikariConfigMap.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
		hikariConfigMap.put("dataSource.url", "jdbc:mysql://106.14.248.228:23306/data_warehouse");
		hikariConfigMap.put("dataSource.user", "weiwei.wu");
		hikariConfigMap.put("dataSource.password", "Miweiwei20170711@");
		ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

		String tableName = "sy_count";
		// 初始化映射器
		JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);
		// 添加所有字段
		/*
		 * List<Column> columnSchema = Lists.newArrayList( new Column("user_id",
		 * java.sql.Types.INTEGER), new Column("user_name", java.sql.Types.VARCHAR));
		 * JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(columnSchema);
		 */

		/*
		 * JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider,
		 * simpleJdbcMapper) .withTableName(tableName) .withQueryTimeoutSecs(30);
		 */
		// 非添加所有字段
		JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
				.withInsertQuery("insert into sy_count(id,UV,PV) values (?,?,?)")// insert into sy_count(id,UV,PV)
																					// values (?,?,?)//update sy_count
																					// set UV=?,PV=? where id=?
				.withQueryTimeoutSecs(30);
		return userPersistanceBolt;

	}

	private static JdbcUpdateBolt MysqlupdateBoltUntil() {
		Map<String, Object> hikariConfigMap = Maps.newHashMap();
		hikariConfigMap.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
		hikariConfigMap.put("dataSource.url", "jdbc:mysql://106.14.248.228:23306/data_warehouse");
		hikariConfigMap.put("dataSource.user", "weiwei.wu");
		hikariConfigMap.put("dataSource.password", "Miweiwei20170711@");
		ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

		String tableName = "sy_count";
		JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);

		JdbcUpdateBolt jdbcUpdateBolt = new JdbcUpdateBolt(connectionProvider, simpleJdbcMapper)
				.withInsertQuery("update sy_count set UV=?,PV=? where id=?")// insert into sy_count(id,UV,PV) values
																			// (?,?,?)//update sy_count set UV=?,PV=?
																			// where id=?
				.withQueryTimeoutSecs(30);// insert into sy_count(id,UV,PV) values (?,?,?)//update sy_count set
											// UV=?,PV=? where id=?
		return jdbcUpdateBolt;

	}

}
