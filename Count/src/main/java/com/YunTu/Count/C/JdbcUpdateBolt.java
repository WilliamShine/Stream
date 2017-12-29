package com.YunTu.Count.C;


import org.apache.commons.lang3.StringUtils;
import org.apache.storm.jdbc.bolt.AbstractJdbcBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.Util;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class JdbcUpdateBolt extends AbstractJdbcBolt{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(JdbcUpdateBolt.class);

    private String tableName;
    private String insertQuery;
    private JdbcMapper jdbcMapper;

    public JdbcUpdateBolt(ConnectionProvider connectionProvider,  JdbcMapper jdbcMapper) {
        super(connectionProvider);
        this.jdbcMapper = jdbcMapper;
    }

    public JdbcUpdateBolt withTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public JdbcUpdateBolt withInsertQuery(String insertQuery) {
        this.insertQuery = insertQuery;
        return this;
    }

    public JdbcUpdateBolt withQueryTimeoutSecs(int queryTimeoutSecs) {
        this.queryTimeoutSecs = queryTimeoutSecs;
        return this;
    }
    
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        super.prepare(map, topologyContext, collector);
        if(StringUtils.isBlank(tableName) && StringUtils.isBlank(insertQuery)) {
            throw new IllegalArgumentException("You must supply either a tableName or an insert Query.");
        }
    }

    public void execute(Tuple tuple) {
        try {
        	/*int queryTimeoutSecs = 30;
            connectionProvider.prepare();
            JdbcClient client = new JdbcClient(connectionProvider, queryTimeoutSecs);
            List<Column> schemaColumns = client.getColumnSchema(tableName);//simpleMaple源码当中获取到的是依次排列顺序的列排序
            System.out.println("----------schemaColumns"+schemaColumns);*/
            
            List<Column> columns = jdbcMapper.getColumns(tuple);//根据表的列序取tuple中的数据依次放进去！！！
            
            exchangerAtTuple(columns,tuple);//按tuple的顺序排列columns
            
            List<List<Column>> columnLists = new ArrayList<List<Column>>();
            columnLists.add(columns);
           /* if(StringUtils.isNotBlank(tableName)) {
                this.jdbcClient.insert(this.tableName, columnLists);//有表名的，直接向表中输出数据就行了
            } else {
                this.jdbcClient.executeInsertQuery(this.insertQuery, columnLists);//没表名的，解析sql命令语句
            }*/
            executeUpdateQuery(this.insertQuery, columnLists);
            this.collector.ack(tuple);
        } catch (Exception e) {
            this.collector.reportError(e);
            this.collector.fail(tuple);
        }
    }

    private void exchangerAtTuple(List<Column> columns, Tuple tuple) {
    	//假设tuple的数据最后一位是表列columns的第一位(其实是针对id)
    	Column column = columns.get(0);
    	columns.remove(0);
    	columns.add(column);
	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
	
    public void executeUpdateQuery(String query, List<List<Column>> columnLists) {
        Connection connection = null;
        try {
            connection = connectionProvider.getConnection();
            boolean autoCommit = connection.getAutoCommit();//返回连接点的自动提交模式的状态
            if(autoCommit) {
                connection.setAutoCommit(false);//将连接点的自动提交模式设为false
            }

            LOG.debug("Executing query {}", query);

            //预编译sql语句，并存储在一个preparedStatement对象当中，用此对象多次执行语句（此处DDL语句与驱动有关，有隐患）
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            if(queryTimeoutSecs > 0) {
                preparedStatement.setQueryTimeout(queryTimeoutSecs);//设置驱动等待的时间
            }

            
            for(List<Column> columnList : columnLists) {
                setPreparedStatementParams(preparedStatement, columnList);//将值赋予preparedStatement
                preparedStatement.addBatch();
            }
            int results = preparedStatement.executeUpdate();//提交命令，执行sql
            if(Arrays.asList(results).contains(Statement.EXECUTE_FAILED)) {
                connection.rollback();
                throw new RuntimeException("failed at least one sql statement in the batch, operation rolled back.");
            } else {
                try {
                    connection.commit();
                } catch (SQLException e) {
                    throw new RuntimeException("Failed to commit insert query " + query, e);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute insert query " + query, e);
        } finally {
            closeConnection(connection);
        }
    }
    
    /**
     * PreparedStatement执行语句，将值依次赋予执行语句PreparedStatement
     * @param preparedStatement
     * @param columnList
     * @throws SQLException
     */
    private void setPreparedStatementParams(PreparedStatement preparedStatement, List<Column> columnList) throws SQLException {
        int index = 1;
        for (Column column : columnList) {
            Class columnJavaType = Util.getJavaType(column.getSqlType());//根据sql数据类型，对应出java类型（类型.class，例如：String.class）
            if (column.getVal() == null) {
                preparedStatement.setNull(index, column.getSqlType());
            } else if (columnJavaType.equals(String.class)) {
                preparedStatement.setString(index, (String) column.getVal());
            } else if (columnJavaType.equals(Integer.class)) {
                preparedStatement.setInt(index, (Integer) column.getVal());
            } else if (columnJavaType.equals(Double.class)) {
                preparedStatement.setDouble(index, (Double) column.getVal());
            } else if (columnJavaType.equals(Float.class)) {
                preparedStatement.setFloat(index, (Float) column.getVal());
            } else if (columnJavaType.equals(Short.class)) {
                preparedStatement.setShort(index, (Short) column.getVal());
            } else if (columnJavaType.equals(Boolean.class)) {
                preparedStatement.setBoolean(index, (Boolean) column.getVal());
            } else if (columnJavaType.equals(byte[].class)) {
                preparedStatement.setBytes(index, (byte[]) column.getVal());
            } else if (columnJavaType.equals(Long.class)) {
                preparedStatement.setLong(index, (Long) column.getVal());
            } else if (columnJavaType.equals(Date.class)) {
                preparedStatement.setDate(index, (Date) column.getVal());
            } else if (columnJavaType.equals(Time.class)) {
                preparedStatement.setTime(index, (Time) column.getVal());
            } else if (columnJavaType.equals(Timestamp.class)) {
                preparedStatement.setTimestamp(index, (Timestamp) column.getVal());
            } else {
                throw new RuntimeException("Unknown type of value " + column.getVal() + " for column " + column.getColumnName());
            }
            ++index;
        }
    }
    
    private void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new RuntimeException("Failed to close connection", e);
            }
        }
    }

	@Override
	protected void process(Tuple tuple) {
		
	}
}
