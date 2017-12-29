package com.YunTu.TopoLine.SelectDriver.bolt1;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.YunTu.TopoLine.SelectDriver.tool.PinyinTool;
import com.YunTu.TopoLine.SelectDriver.tool.PinyinTool.Type;

import net.sf.json.JSONObject;
import net.sourceforge.pinyin4j.format.exception.BadHanyuPinyinOutputFormatCombination;


/**
 * 切分格式转换
 * @author 84031
 *
 */
public class DateFmtBolt extends BaseRichBolt {

	 /** 
     *  
     */  
     private static final long serialVersionUID = 1L; 
     public static Logger LOG = LoggerFactory.getLogger(DateFmtBolt.class);
     private int release_dateN ;
     private int hit_tagN ;
     private String dwtvs_type ;
  
     OutputCollector collector;  
     String famtNoType;
     String jsonStr;
     Map<String, String> Limitmap;
     Map<String, Object> jsonMap;
     Map<String, List<String>> DimensionMap;
     String year;
     String mouth;
     String day;
     public DateFmtBolt(String famtNoType) {
		this.famtNoType=famtNoType;
	}
     
     public void prepare(Map map, TopologyContext context, OutputCollector collector) {
    	 this.collector = collector;  
    	 release_dateN = Integer.parseInt((String) map.get(OriginalTopology.T_EBIZ_COMMENT_RELEASE_DATE));
    	 hit_tagN=Integer.parseInt((String) map.get(OriginalTopology.T_EBIZ_COMMENT_HIT_TAG));
    	 dwtvs_type=(String) map.get(OriginalTopology.DSSLRESULT_DWTVS_TYPE);
    	 
    	 jsonStr=(String) map.get(OriginalTopology.TOPOLOGY_WORK_JOSN);
    	 JSONObject json = JSONObject.fromObject(jsonStr);
    	 jsonMap = (Map<String, Object>) json.get(famtNoType);//获得该类对应的json
    	 DimensionMap=(Map<String, List<String>>) jsonMap.get("维度");//限制维度
    	 List<String> Limitset=(List<String>) jsonMap.get("限制条件");//["WHERE year='2017'","WHERE mouth='11'"]
    	 Limitmap=limitchange(Limitset);//{mouth=11, year=2017}
    	 if (Limitmap.containsKey("day")) {
			} else if (Limitmap.containsKey("mouth")) {
				day = "0";
			} else if (Limitmap.containsKey("year")) {
				mouth = "0";
				day = "0";
			}
     }
     /**
      * 限制对应转换
      * @param Limitset
      * @return
      */
     private  Map<String, String> limitchange(List<String> Limitset) {
    	 Map<String, String> map = new HashMap<String, String>();
    	 for (String str : Limitset) {
    		 if (str.startsWith("WHERE year='")&&str.endsWith("'")) {
    			 map.put("year", str.replace("WHERE year='", "").replace("'", ""));
			} else if (str.startsWith("WHERE mouth='")&&str.endsWith("'")) {
				 map.put("mouth", str.replace("WHERE mouth='", "").replace("'", ""));
			}else if (str.startsWith("WHERE day='")&&str.endsWith("'")){
				 map.put("day", str.replace("WHERE day='", "").replace("'", ""));
			}
		}
    	 
    	 
		return map;
	}
     
	public void execute(Tuple tuple) {
		try {
			String IDstr = sliceFormat(tuple);//资源	//33
			if (!(IDstr==null)) {
				this.collector.emit(new Values(IDstr));//不带空统计
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("DateFmtBolt is failure.date ######:tuple:" + tuple );
			this.collector.fail(tuple);
		}
		this.collector.ack(tuple);
	}
	
	
	
	public String sliceFormat(Tuple tuple) {
		String line = tuple.getString(0);
		String[] lines = line.split("\t", -1);
		String release_date = lines[release_dateN]; //35
		String hit_tag = lines[hit_tagN];//33
		Calendar calendar = null;
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			calendar = Calendar.getInstance();
			calendar.setTime(sdf.parse(release_date));
			String[] hit_tags = hit_tag.split("/", -1);
			String tag = hit_tags[3];
			boolean isEmit = true;
			isEmit=DimensionMap.get("hit_tag").contains(tag);
			if (year==null) {
				year=calendar.get(Calendar.YEAR)+"";
				if (!year.equals(Limitmap.get("year"))) {
					isEmit=false;
				}
			}
			if (mouth==null) {
				mouth=calendar.get(Calendar.MONTH) + 1+"";
				if (!mouth.equals(Limitmap.get("mouth"))) {
					isEmit=false;
				}
			}
			if (day==null) {
				day=calendar.get(Calendar.DAY_OF_MONTH)+"";
				if (!day.equals(Limitmap.get("day"))) {
					isEmit=false;
				}
			}
			if (isEmit) {
				PinyinTool tool = new PinyinTool();  
				tag=tool.toPinYin(tag, "", Type.LOWERCASE);
				String tags = 
						jsonMap.get("任务号")+OriginalTopology.segmentation
						+"DSSL"+OriginalTopology.segmentation
						+tag+OriginalTopology.segmentation
						+year+OriginalTopology.segmentation
						+mouth+OriginalTopology.segmentation
						+day+OriginalTopology.segmentation//day
						+"0"+OriginalTopology.segmentation
						+jsonMap.get("业务类型");//"/eb/"
    			return tags;
			} else {
				return null;
			}
		} catch (ParseException e) {
			LOG.error("时间无法格式化 " + "\t  line" + line);
			e.printStackTrace();
			return null;
		} catch (BadHanyuPinyinOutputFormatCombination e) {
			LOG.error("tag转拼英错误 " + "\t  line" + line);
			e.printStackTrace();
			return null;
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		 declarer.declare(new Fields("id"));  
	}
	
}
