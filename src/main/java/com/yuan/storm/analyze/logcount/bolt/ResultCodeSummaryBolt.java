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
	private int resultCode = 0;
	private long msgTS = 0l;
	private int count = 0;
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
        
        for (Tuple tuple : newTuples) {
        	result = (String) tuple.getValue(0);
        	resultCode = Integer.parseInt(result);
        	if (10000 == resultCode) {
        		count++;
        	}
        }
        
        msgTS = (long) tuplesInWindow.get(0).getValueByField("ts");
        
        ts = System.currentTimeMillis();
        
        Map<String, Object> counts = new HashMap<String, Object>();
        counts.put("longin_success", count);
        counts.put("timestamp", new Date(msgTS));
        collector.emit(new Values(counts, new Date(ts)));
        System.out.println("ResultCodeSummaryBolt count: " + count);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("count", "ts"));
	}

}
