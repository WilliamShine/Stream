package com.YunTu.TopoLine.SelectDriver.bolt3;

import java.util.Map;
import java.util.Set;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.YunTu.TopoLine.SelectDriver.OriginalTopology;



/**
 * 切分格式转换
 * @author 84031
 *
 */
public class DateFmtBolt3 extends BaseRichBolt {

	 /** 
     *  
     */  
     private static final long serialVersionUID = 1L; 
     public static Logger LOG = LoggerFactory.getLogger(DateFmtBolt3.class);
     Map<String, Set<String>> urlmap = null;
     Set<String> set =null;
     private int t_comment_release_dateN ;
     private int t_comment_c_url_crcN ;
     private int dt3_t_data_url_crc ;
     private int dt3_t_data_rel_type ;
     private int dt3_t_label_url_crc ;
     private int dt3_t_label_hit_tag ;
     private String dwtvs_type ;
     
     private String t_data_hdfsIP ;
     private String t_data_PATH;
     private String t_label_hdfsIP ;
     private String t_label_PATH;
     
     OutputCollector collector;  
     public void prepare(Map map, TopologyContext context, OutputCollector collector) {
    	 this.collector = collector; 
    	 t_comment_release_dateN = Integer.parseInt((String) map.get(OriginalTopology.T_COMMENT_RELEASE_DATE));
    	 t_comment_c_url_crcN=Integer.parseInt((String) map.get(OriginalTopology.T_COMMENT_C_URL_CRC));
    	 dt3_t_data_url_crc = Integer.parseInt((String) map.get(OriginalTopology.DT3_T_DATA_URL_CRC));
    	 dt3_t_data_rel_type=Integer.parseInt((String) map.get(OriginalTopology.DT3_T_DATA_REL_TYPE));
    	 dt3_t_label_url_crc = Integer.parseInt((String) map.get(OriginalTopology.DT3_T_LABEL_URL_CRC));
    	 dt3_t_label_hit_tag = Integer.parseInt((String) map.get(OriginalTopology.DT3_T_LABEL_HIT_TAG));
    	 dwtvs_type=(String) map.get(OriginalTopology.FDSPLSLRESULT_DWTVS_TYPE);

    	 t_data_hdfsIP = (String) map.get(OriginalTopology.FDSPLSL_T_DATA_HDFSIP);
    	 t_data_PATH = (String) map.get(OriginalTopology.FDSPLSL_T_DATA_PATH);
    	 t_label_hdfsIP = (String) map.get(OriginalTopology.FDSPLSL_T_LABEL_HDFSIP);
    	 t_label_PATH = (String) map.get(OriginalTopology.FDSPLSL_T_LABEL_PATH);
    	 
    	 urlmap=new WhereUrl3().getN2ResultUrl(t_data_hdfsIP,t_data_PATH,dt3_t_data_url_crc,dt3_t_data_rel_type,t_label_hdfsIP,t_label_PATH,dt3_t_label_url_crc,dt3_t_label_hit_tag);
    	 set = urlmap.get("/声量/小米");
     }
     
	public void execute(Tuple tuple) {
		String line = tuple.getString(0);
		String[] lines = line.split("\t", -1);
		if (lines != null && lines.length == 61) {
			String release_date = lines[t_comment_release_dateN];//5
			String c_url_crc = lines[t_comment_c_url_crcN];//44
			if (set.contains(c_url_crc)) {
				String[] daydates = release_date.split(" ", -1);
				if (daydates != null && daydates.length >= 2) {
					String[] dates = daydates[0].split("-", -1);
					if (dates != null && dates.length >= 3) {
						String year = dates[0];
						String mouth = dates[1];
						String day = dates[2];

						String tags = 
								"DT01" + OriginalTopology.segmentation 
								+ "FDSPLSL" + OriginalTopology.segmentation
								+ "XiaoMi" + OriginalTopology.segmentation 
								+ year + OriginalTopology.segmentation
								+ mouth + OriginalTopology.segmentation 
								+ "0" + OriginalTopology.segmentation 
								+ "0" + OriginalTopology.segmentation 
								+ dwtvs_type;//"/neb/comment_volume/"
						
						/*
						 * if (hit_tag.indexOf("/小米") != -1) { tags
						 * +=OriginalTopology.segmentation+"小米"; } else if
						 * (hit_tag.indexOf("/夏普") != -1) { tags
						 * +=OriginalTopology.segmentation+"夏普"; } else { tags
						 * +=OriginalTopology.segmentation+"其他"; }
						 *//*
							 * else if (hit_tag.indexOf("海信") != -1) { //TODO }
							 * else if (hit_tag.indexOf("风行") != -1) { //TODO }
							 */
						try {
							this.collector.emit(new Values(tags));
						} catch (Exception e) {
							e.printStackTrace();
							this.collector.fail(tuple);
						}
					} else {
						LOG.error("时间-分割不足3" + "\t  line" + line);
					}
				} else {
					LOG.error("时间 分割不足2 \t  release_date" + "\t" + line);
				}
			}
		} else {
			LOG.error("数据tab分割不等于61 \t  line \t" + line);
		}
		this.collector.ack(tuple);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		 declarer.declare(new Fields("id"));  
	}

}
