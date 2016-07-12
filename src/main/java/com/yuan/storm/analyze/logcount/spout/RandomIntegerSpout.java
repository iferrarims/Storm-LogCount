package com.yuan.storm.analyze.logcount.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * Emits a random integer and a timestamp value (offset by one day),
 * every 100 ms. The ts field can be used in tuple time based windowing.
 */
public class RandomIntegerSpout extends BaseRichSpout {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(RandomIntegerSpout.class);
    private SpoutOutputCollector collector;
    private Random rand;
    private long msgId = 0;
    private long ts = 0L; 

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("value", "ts", "msgid"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.rand = new Random();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(500);
//        collector.emit(new Values(rand.nextInt(1000), System.currentTimeMillis() - (24 * 60 * 60 * 1000), ++msgId), msgId);
//        collector.emit(new Values(rand.nextInt(1000), ts=System.currentTimeMillis(), ++msgId), msgId);
        //时间戳加随机值
//        collector.emit(new Values(1, ts=System.currentTimeMillis() + rand.nextInt(5000), ++msgId), msgId);
//        int i = -rand.nextInt(12000) + 6000;
        int i = -rand.nextInt(8000);
        System.out.println("i = " + i);
        collector.emit(new Values(1, ts=System.currentTimeMillis() + i, ++msgId), msgId);
//        collector.emit(new Values(1, ts = System.currentTimeMillis(), ++msgId), msgId);
//        System.out.println("current ts: " + System.currentTimeMillis());
        System.out.printf("randomSpout emit message ts:%d msgID:%d\r\n", ts, msgId);
    }

    @Override
    public void ack(Object msgId) {
        LOG.debug("Got ACK for msgId : " + msgId);
    }

    @Override
    public void fail(Object msgId) {
        LOG.debug("Got FAIL for msgId : " + msgId);
    }
}
