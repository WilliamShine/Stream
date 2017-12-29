package com.YunTu.Storm111.uvCount.FFF;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordCounter;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlobalCountBolt1 extends BaseRichBolt {
    public static Logger LOG = LoggerFactory.getLogger(TestWordCounter.class);

    private int _count;
    OutputCollector _collector;

    public void cleanup() {

    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        _count = 0;
    }

    public void execute(Tuple input) {
        _count++;
        _collector.emit(input, new Values(_count));
        System.out.println(input+"输出总和:"+new Values(_count));
        _collector.ack(input);
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("global-count"));
    }
    
    public Fields getOutputFields() {
        return new Fields("global-count");
    }

    
}
