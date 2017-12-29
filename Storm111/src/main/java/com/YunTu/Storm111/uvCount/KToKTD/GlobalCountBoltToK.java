package com.YunTu.Storm111.uvCount.KToKTD;

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

public class GlobalCountBoltToK extends BaseRichBolt {
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
        _collector.emit(input, new Values("AA",_count+""));
        LOG.info(input+"输出总和:"+new Values(_count));
        _collector.ack(input);
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key","message"));
    }
    
    public Fields getOutputFields() {
        return new Fields("global-count");
    }

    
}
