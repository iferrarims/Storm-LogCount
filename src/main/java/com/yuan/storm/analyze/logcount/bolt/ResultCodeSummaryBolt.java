package com.yuan.storm.analyze.logcount.bolt;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import scala.collection.SeqViewLike.Mapped;

public class ResultCodeSummaryBolt extends BaseWindowedBolt {

	private static final long serialVersionUID = -2847097659094404675L;
	private OutputCollector collector;
	private String result = null;
	private long msgTS = 0l;
	private long ts = 0;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(TupleWindow inputWindow) {
		List<Tuple> tuplesInWindow = inputWindow.get();
        List<Tuple> newTuples = inputWindow.getNew();
        List<Tuple> expiredTuples = inputWindow.getExpired();
        
        int Logincount = 0;
        int registerCount = 0;
        
        for (Tuple tuple : newTuples) {
        	result = (String) tuple.getValue(0);
        	if (result.equals("10000")) {
        		Logincount++;
        	}
        	else if (result.equals("10002")) {
        		registerCount++;
        	}
        }
        
        msgTS = (long) tuplesInWindow.get(0).getValueByField("ts");
        
        ts = System.currentTimeMillis();
        
        if (0 != Logincount) {        
	        Map<String, Object> counts = new HashMap<String, Object>();
	        counts.put("login_success", Logincount);
	        counts.put("timestamp", new Date(msgTS));
	        collector.emit(new Values(counts, new Date(ts)));
	        System.out.println("ResultCodeSummaryBolt count: " + Logincount + " Events in current window: " + tuplesInWindow.size());
        }
        
        if (0 != registerCount) {
	        Map<String, Object> registerCounts = new HashMap<String, Object>();
	        registerCounts.put("register_success", registerCount);
	        registerCounts.put("timestamp", new Date(msgTS));
	        collector.emit(new Values(registerCounts, new Date(ts)));
	        System.out.println("ResultCodeSummaryBolt register count : " + registerCount);
        }
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("count", "ts"));
	}

}
