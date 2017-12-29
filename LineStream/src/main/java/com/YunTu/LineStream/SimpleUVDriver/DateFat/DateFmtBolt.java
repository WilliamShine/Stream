package com.YunTu.LineStream.SimpleUVDriver.DateFat;

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

import com.YunTu.LineStream.SimpleUVDriver.DemoDriver;


public class DateFmtBolt extends BaseRichBolt {

	/** 
	*  
	*/
	private static final long serialVersionUID = 1L;
	public static Logger LOG = LoggerFactory.getLogger(DateFmtBolt.class);
	private int IDNo;
	/*private int release_dateN;
	private int hit_tagN;
	private String dwtvs_type;*/

	OutputCollector collector;
	DataSpliceFarmat datafarmat;
	String IDstr;

	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.datafarmat = new DataSpliceFarmat();
		System.out.println(map.get(DemoDriver.ID_NUMBER));
//		IDNo=Integer.parseInt((String)map.get(DemoDriver.ID_NUMBER));
		/*
		 * release_dateN = Integer.parseInt((String)
		 * map.get(OriginalTopology.T_EBIZ_COMMENT_RELEASE_DATE));
		 * hit_tagN=Integer.parseInt((String)
		 * map.get(OriginalTopology.T_EBIZ_COMMENT_HIT_TAG));
		 * dwtvs_type=(String) map.get(OriginalTopology.DSSLRESULT_DWTVS_TYPE);
		 */
	}

	public void execute(Tuple tuple) {
		try {
			IDstr = datafarmat.sliceFormat(tuple,1);//资源	//33
			this.collector.emit(new Values(IDstr));//带空统计
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("DateFmtBolt is failure.date ######:tuple:" + tuple );
			this.collector.fail(tuple);
		}
		this.collector.ack(tuple);
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id"));
	}

}
