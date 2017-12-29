package com.YunTu.LineStream.Spout;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.YunTu.LineStream.resource.ShareResource;

public class ResourceReadSpout extends BaseRichSpout {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(ResourceReadSpout.class);

    SpoutOutputCollector _collector;
    
    long _lastUpdateMs = System.currentTimeMillis();
    long _stateUpdateIntervalMs;
    long nowTime;
    long diffWithNow;

    int _currPartitionIndex = 0;
    
    public ResourceReadSpout(int stateUpdateIntervalMs) {
    	_stateUpdateIntervalMs=stateUpdateIntervalMs;
    }
    
    
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		 _collector = collector;
		
	}

	@Override
	public void nextTuple() {
		try {
//			Utils.sleep(1000);  
			nowTime = System.currentTimeMillis();
			diffWithNow = nowTime - _lastUpdateMs;
			if (diffWithNow > _stateUpdateIntervalMs || diffWithNow < 0) {
//				if () {//非实时，准实时策略ShareResource.isUpdateFlag()
				//TODO
				Long pv = ShareResource.findLongValue("PV");
				Long uv = ShareResource.findLongValue("UV");
				/*System.out.println("PV:"+pv+"----UV:"+uv+"		"+ShareResource.getMap().size());*/
					_lastUpdateMs = nowTime;// 更新策略_lastUpdateMs=System.currentTimeMillis();
//					ShareResource.setUpdateFlag(false);//准实时策略，存在先后差距
//				}
					_collector.emit(new Values(pv,uv));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("PV","UV"));  
		
	}
	@Override
    public void ack(Object msgId) {
        //TODO
    }
	
	@Override
    public void fail(Object msgId) {
       //TODO
    }


}
