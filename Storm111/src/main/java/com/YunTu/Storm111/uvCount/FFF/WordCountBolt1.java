package com.YunTu.Storm111.uvCount.FFF;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordCounter;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountBolt1 extends BaseRichBolt {
    public static Logger LOG = LoggerFactory.getLogger(TestWordCounter.class);

    private int _count;
    OutputCollector _collector;


    public void cleanup() {

    }

    public Fields getOutputFields() {
        return new Fields("global-count");
    }

    
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        _count = 0;
        System.out.println("************prepare 方法*******************");
    }

    private Map<String, Integer> map = new HashMap<String, Integer>();
    private int num = 1;
    public void execute(Tuple input) {
    	String line = input.getString(0);
    	if (map.containsKey(line)) {
    		num=map.get(line)+1;
		}
    	map.put(line, num);
//        _count++;
        _collector.emit(input, new Values(num));
        System.out.println(input+"第:"+ num);
        _collector.ack(input);
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("global-count"));
    }
    
    @Test
    public void tets() {
    	Map<String, Integer> map = new HashMap<String, Integer>();
    	Integer a = 0;
    	
    	a= map.get("a");
    	if (a==null) {
			System.out.println(00);
		}
    	if (map.containsKey("a")) {
			System.out.println(0);
		}else {
			System.out.println(1);
		}

	}
}
