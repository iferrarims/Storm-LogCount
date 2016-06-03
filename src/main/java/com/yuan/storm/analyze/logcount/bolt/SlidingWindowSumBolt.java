package com.yuan.storm.analyze.logcount.bolt;

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

import java.util.List;
import java.util.Map;

/**
 * Computes sliding window sum
 */
public class SlidingWindowSumBolt extends BaseWindowedBolt {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(SlidingWindowSumBolt.class);

    private int sum = 0;
    private OutputCollector collector;
    private long start = 0l;
    private long end = 0l;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        start = System.currentTimeMillis();
    }

    @Override
    public void execute(TupleWindow inputWindow) {
            /*
             * The inputWindow gives a view of
             * (a) all the events in the window
             * (b) events that expired since last activation of the window
             * (c) events that newly arrived since last activation of the window
             */
        List<Tuple> tuplesInWindow = inputWindow.get();
        List<Tuple> newTuples = inputWindow.getNew();
        List<Tuple> expiredTuples = inputWindow.getExpired();

        LOG.debug("Events in current window: " + tuplesInWindow.size());
            /*
             * Instead of iterating over all the tuples in the window to compute
             * the sum, the values for the new events are added and old events are
             * subtracted. Similar optimizations might be possible in other
             * windowing computations.
             */
        for (Tuple tuple : newTuples) {
            sum += (int) tuple.getValue(0);
            System.out.println("SumBolt: current added msg ID: " + tuple.getValue(2));
        }
        for (Tuple tuple : expiredTuples) {
            sum -= (int) tuple.getValue(0);
            System.out.println("SumBolt: current subtracted msg ID: " + tuple.getValue(2) + " ts: " + tuple.getValue(1));
        }
        collector.emit(new Values(sum));
        end = System.currentTimeMillis();
        System.out.println("SumBolt: Events in current window: " + tuplesInWindow.size() + " ts: " + end +  " use time: " + (end - start));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sum"));
    }
}
