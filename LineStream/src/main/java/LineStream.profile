# The topology run model."online" or "local"
topology_mode=local

#KafkaSpout name in Topology and parallelism number.It is use for balance.
kafka_spout1_name=kafka_spout1
kafka_spout1_parallelism_number=3
datefmtbolt1_parallelism_number=3
kafka_spout1_zkConn=node01.cdh1:2181,node02.cdh1:2181,node03.cdh1:2181
kafka_spout1_topicname=t_ebiz_comment
#t_ebiz_comment
kafka_spout1_brokerzkpath=/kafka/brokers
kafka_spout1_start_offset_time=kafka.api.OffsetRequest.EarliestTime()
t_ebiz_comment.release_date=35
t_ebiz_comment.hit_tag=33
DSSLresult.dwtvs_type=/eb/

#kafka_spout2_name=kafka_spout2
#kafka_spout2_parallelism_number=3
#datefmtbolt2_parallelism_number=3
#kafka_spout2_zkConn=node01.cdh1:2181,node02.cdh1:2181,node03.cdh1:2181
#kafka_spout2_topicname=t1
##t_data
#kafka_spout2_brokerzkpath=/kafka/brokers
#kafka_spout2_start_offset_time=kafka.api.OffsetRequest.EarliestTime()
#t_data.release_date=1
#t_data.url_crc=0
#t_data.rel_type=2
#t_label.url_crc=3
#t_label.hit_tag=5
#fdsztsl_result.dwtvs_type=/neb/theme_volume/
#fdsztsl_t_label_hdfsip=hdfs://node01.cdh1:8020
#fdsztsl_T_label_path=/user/shine/1407714309205630_76131_10214/t_label

#kafka_spout3_name=kafka_spout3
#kafka_spout3_parallelism_number=3
#datefmtbolt3_parallelism_number=3
#kafka_spout3_zkConn=node01.cdh1:2181,node02.cdh1:2181,node03.cdh1:2181
#kafka_spout3_topicname=t_comment
##t_comment
#kafka_spout3_brokerzkpath=/kafka/brokers
#kafka_spout3_start_offset_time=kafka.api.OffsetRequest.EarliestTime()
#t_comment.release_date=5
#t_comment.c_url_crc=44
#dt3_t_data.url_crc=0
#dt3_t_data.rel_type=2
#dt3_t_label.url_crc=3
#dt3_t_label.hit_tag=5
#fdsplsl_result.dwtvs_type=/neb/comment_volume/
#fdsplsl_t_data_hdfsip=hdfs://node01.cdh1:8020/
#fdsplsl_t_data_path=/user/shine/1407714309205630_76131_10214/t_data_3
#fdsplsl_t_label_hdfsip=hdfs://node01.cdh1:8020
#fdsplsl_T_label_path=/user/shine/1407714309205630_76131_10214/t_label

deepvisitbolt_parallelism_number=3
deep_upvsumbolt_parallelism_number=1

#Confs in Topology and transfer in map
topology_workers_number=4
deepbolt_interval_time=20
deepbolt_interval_num=100
topology_debug_mode=false
id.number=1

#MySQL connection profile
mysql_out_bolt_name=mysql_out_bolt
mysql_write_bolt_parallelism_number=1
dataSourceClassName=com.mysql.jdbc.jdbc2.optional.MysqlDataSource
dataSource.url=jdbc:mysql://47.100.9.7:3306/data_warehouse
dataSource.user=weiwei.wu
dataSource.password=Miweiwei20170711@
table.name=simpleUVPV
update.query=update simpleUVPV set total_pv=?,total_uv=?,last_update=? where task_Number=?
query_timeout_secs=30