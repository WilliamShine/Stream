package com.YunTu.TopoLine.SelectDriver.bolt2;

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
public class DateFmtBolt2 extends BaseRichBolt {

	 /** 
     *  
     */  
     private static final long serialVersionUID = 1L; 
     public static Logger LOG = LoggerFactory.getLogger(DateFmtBolt2.class);
     Map<String, Set<String>> usrmap = null;
     Set<String> set =null;
     private int t_data_release_dateN ;
     private int t_data_url_crcN ;
     private int t_data_rel_typeN ;
     private int t_label_url_crcN ;
     private int t_label_hit_tagN ;
     private String dwtvs_type ;
     private String hdfsIP ;
     private String t_labelPATH;
     OutputCollector collector;  
     public void prepare(Map map, TopologyContext context, OutputCollector collector) {
    	 this.collector = collector;
    	 t_data_release_dateN = Integer.parseInt((String) map.get(OriginalTopology.T_DATA_RELEASE_DATE));
    	 t_data_url_crcN=Integer.parseInt((String) map.get(OriginalTopology.T_DATA_URL_CRC));
    	 t_data_rel_typeN = Integer.parseInt((String) map.get(OriginalTopology.T_DATA_REL_TYPE));
    	 t_label_url_crcN=Integer.parseInt((String) map.get(OriginalTopology.T_LABEL_URL_CRC));
    	 t_label_hit_tagN = Integer.parseInt((String) map.get(OriginalTopology.T_LABEL_HIT_TAG));
    	 dwtvs_type=(String) map.get(OriginalTopology.FDSZTSL_RESULT_DWTVS_TYPE);

    	 hdfsIP = (String) map.get(OriginalTopology.FDSZTSL_T_LABEL_HDFSIP);
    	 t_labelPATH = (String) map.get(OriginalTopology.FDSZTSL_T_LABEL_PATH);
    	 usrmap=new WhereUrl2().getUrl(hdfsIP,t_labelPATH,t_label_hit_tagN,t_label_url_crcN);//"hdfs://node01.cdh1:8020",
			//"/user/shine/1407714309205630_76131_10214/t_label"
    	 set = usrmap.get("/声量/小米");
     }
     
	public void execute(Tuple tuple) {
		String line = tuple.getString(0);
		String[] lines = line.split("\t", -1);
		if (lines != null && lines.length == 3) {//62元数据列号
			String url_crc = lines[t_data_url_crcN];//1//0
			String release_date = lines[t_data_release_dateN];//6//1
			String rel_type = lines[t_data_rel_typeN];//10//2
			if (rel_type.equals("m") || rel_type.equals("q")) {
				if (set.contains(url_crc)) {
					String[] daydates = release_date.split(" ", -1);
					if (daydates != null && daydates.length >= 2) {
						String[] dates = daydates[0].split("-", -1);
						if (dates != null && dates.length >= 3) {
							String year = dates[0];
							String mouth = dates[1];
							String day = dates[2];

							String tags = 
									"DT01" + OriginalTopology.segmentation 
									+ "FDSZTSL"+ OriginalTopology.segmentation 
									+ "XiaoMi" + OriginalTopology.segmentation 
									+ year+ OriginalTopology.segmentation 
									+ mouth + OriginalTopology.segmentation 
									+ "0" + OriginalTopology.segmentation 
									+ "0" + OriginalTopology.segmentation
									+ dwtvs_type;

							/*
							 * if (hit_tag.indexOf("/小米") != -1) { tags
							 * +=OriginalTopology.segmentation+"小米"; } else if
							 * (hit_tag.indexOf("/夏普") != -1) { tags
							 * +=OriginalTopology.segmentation+"夏普"; } else {
							 * tags +=OriginalTopology.segmentation+"其他"; }
							 *//*
								 * else if (hit_tag.indexOf("海信") != -1) {
								 * //TODO } else if (hit_tag.indexOf("风行") !=
								 * -1) { //TODO }
								 */
							try {
								this.collector.emit(new Values(tags));
							} catch (Exception e) {
								e.printStackTrace();
								this.collector.fail(tuple);
							}
						} else {
							LOG.error("时间-分割不足3"+"\t  line"+line);
						}

					} else {
						LOG.error("时间 分割不足2 \t  release_date"+"\t"+line);
					}
				}
			}
		} else {
			LOG.error("数据tab分割不等于3 \t  line \t"+line);
		}
		this.collector.ack(tuple);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		 declarer.declare(new Fields("id"));  
	}

}
