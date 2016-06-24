package com.yuan.storm.analyze.logcount.bolt;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

public class ProjectCountBolt extends BaseWindowedBolt {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(ProjectCountBolt.class);

    private int sum = 0;
    private OutputCollector collector;
    private String proName = null;
    private long ts = 0;
    private long msgTS = 0l;
//    private Jedis jedis = null;
    
    @Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
//		jedis = new Jedis("172.28.29.151", 6379);
	}
    

	@Override
	public void execute(TupleWindow inputWindow) {
		List<Tuple> tuplesInWindow = inputWindow.get();
        List<Tuple> newTuples = inputWindow.getNew();
        List<Tuple> expiredTuples = inputWindow.getExpired();
        Map<String, Integer> counts = new HashMap<String, Integer>();
        
        for (Tuple tuple : newTuples) {
        	proName = (String) tuple.getValue(0);
        	Integer count = counts.get(proName);
        	if (count == null) {
        		count = 0;
        	}
        	count++;
        	counts.put(proName, count);	
//        	System.out.println("ProjectCountBolt: msgID " + tuple.getValue(2));
        }
        
        msgTS = (long) newTuples.get(0).getValueByField("ts");
        
//        Iterator iter = counts.entrySet().iterator();
//        while (iter.hasNext()) {
//        	Map.Entry<String, Integer> entry = (Entry<String, Integer>) iter.next();
//        	this.jedis.zadd("s_server", entry.getValue(), entry.getKey());
////        	System.out.println("ProjectCountBolt : value " + entry.getValue() + " key " + entry.getKey());
//        }
        
        ts = System.currentTimeMillis();
        collector.emit(new Values(counts, ts, "project_name"));
        System.out.println("ProjectCountBolt: Events in current window: " + tuplesInWindow.size() + " timestamp: " + System.currentTimeMillis() + " msgTS" + msgTS);
		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sumMap", "ts", "name"));
	}

}
