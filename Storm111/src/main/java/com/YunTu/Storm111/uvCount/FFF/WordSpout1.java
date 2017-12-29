package com.YunTu.Storm111.uvCount.FFF;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordSpout1 extends BaseRichSpout {
    public static Logger LOG = LoggerFactory.getLogger(WordSpout1.class);
    boolean _isDistributed;
    SpoutOutputCollector _collector;

    public WordSpout1() {
        this(true);
    }

    public WordSpout1(boolean isDistributed) {
        _isDistributed = isDistributed;
    }
        
    public void close() {
        
    }
    
    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {
        
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        if(!_isDistributed) {
            Map<String, Object> ret = new HashMap<String, Object>();
            ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
            return ret;
        } else {
            return null;
        }
    }  
    
    
    
    //
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    	_collector = collector;
    }
    
    public void nextTuple() {
    	Utils.sleep(100);
    	final String[] words = new String[] {"nathan", "mike", "jackson", "golda", "bertels"};
    	final Random rand = new Random();
    	final String word = words[rand.nextInt(words.length)];
    	System.out.println("spout输出："+word);
    	_collector.emit(new Values(word));
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
  
}
