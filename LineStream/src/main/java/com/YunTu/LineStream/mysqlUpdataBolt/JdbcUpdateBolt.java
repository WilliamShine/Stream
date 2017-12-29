package com.YunTu.LineStream.mysqlUpdataBolt;

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
            List<Column> columns = jdbcMapper.getColumns(tuple);//根据表的一行列序取tuple中的数据依次放进去！！！,对应必须有值。
            
            exchangerAtTuple(columns,tuple);//按tuple的顺序排列list
            
            List<List<Column>> columnLists = new ArrayList<List<Column>>();
            columnLists.add(columns);
           /* if(StringUtils.isNotBlank(tableName)) {
                this.jdbcClient.insert(this.tableName, columnLists);//insert方法。有表名的，直接向表中输出数据就行了
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

    /**
     * 调换位置，并不是通用的高级方法
     * @param columns
     * @param tuple
     */
    private void exchangerAtTuple(List<Column> columns, Tuple tuple) {
    	Column column = columns.get(0);
    	columns.remove(0);
    	columns.add(column);
//    	columns.add(0, column);
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
            
			for (List<Column> columnList : columnLists) {
				setPreparedStatementParams(preparedStatement, columnList);// 将值赋予preparedStatement
				preparedStatement.addBatch();
			}
            int results = preparedStatement.executeUpdate();//提交命令，执行sql
            
            LOG.debug("sql Updata状态：results"+columnLists.get(0));
            if (results!=1) {
            	//如果没有就插入
            	if (results==0) {
            		String insertQuery = exchanger(query);
            		 /*Connection Newconnection =connectionProvider.getConnection();
            		 boolean INautoCommit = connection.getAutoCommit();//返回连接点的自动提交模式的状态
                     if(INautoCommit) {
                    	 Newconnection.setAutoCommit(false);//将连接点的自动提交模式设为false
                     }*/

                     LOG.debug("Executing insertQuery {}", insertQuery);
            		
            		PreparedStatement preparedStatementInsert = connection.prepareStatement(insertQuery);
                    if(queryTimeoutSecs > 0) {
                    	preparedStatementInsert.setQueryTimeout(queryTimeoutSecs);//设置驱动等待的时间
                    }
                    
        			for (List<Column> columnList : columnLists) {
        				setPreparedStatementParams(preparedStatementInsert, columnList);// 将值赋予preparedStatementInsert
        				preparedStatementInsert.addBatch();
        			}
        			int[] insetResults = preparedStatementInsert.executeBatch();
        			LOG.debug("sql Insert状态：insetResults"+insetResults[0]+columnLists.get(0));
        			if(Arrays.asList(insetResults).contains(Statement.EXECUTE_FAILED)) {
        				connection.rollback();
                        throw new RuntimeException("failed at least one sql statement in the batch, operation rolled back.");
                    } else {
                        try {
                        	connection.commit();
                        } catch (SQLException e) {
                            throw new RuntimeException("Failed to commit insert query " + insertQuery, e);
                        }
                    }
					
				} else {
					LOG.error("结构错误：sql执行Updata操作了多行：results："+results);
				}
				
			}
            
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
     * 将updata语句转为对应的insert语句，同顺序的。
     * @param results
     * @return
     */
    private String exchanger(String query) {
		String[] result = query.split(" ", -1);
		String tableName = result[1];
		List<String> list = new ArrayList<String>();
		for (int i = 0; i < query.length(); i++) {
			if (query.charAt(i)=='?') {
				int s = i;
				while (query.charAt(s--)!= ' ') {
					if (query.charAt(s-1)== ' '||query.charAt(s-1)== ',') {
						list.add(query.substring(s, i-1));
						break;
					}
				}
			}
		}
		String newResults = "insert into "+tableName+"(";
		String num = "";
		for (String string : list) {
			newResults+=string+",";
			num+="?,";
		}
		newResults=newResults.substring(0, newResults.length()-1)+") values ("+num.substring(0, num.length()-1)+")";
		return newResults;
	}
    
    /**
     * PreparedStatement执行语句，将值依次赋予执行语句PreparedStatement
     * 按List的顺序向preparedStatement中set数据
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
