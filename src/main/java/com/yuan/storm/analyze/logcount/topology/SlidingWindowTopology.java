package com.yuan.storm.analyze.logcount.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yuan.storm.analyze.logcount.bolt.PrinterBolt;
import com.yuan.storm.analyze.logcount.bolt.SlidingWindowSumBolt;
import com.yuan.storm.analyze.logcount.spout.RandomIntegerSpout;

import java.util.List;
import java.util.Map;

//import static org.apache.storm.topology.base.BaseWindowedBolt.Count;

/**
 * A sample topology that demonstrates the usage of {@link org.apache.storm.topology.IWindowedBolt}
 * to calculate sliding window sum.
 */
public class SlidingWindowTopology {

    private static final Logger LOG = LoggerFactory.getLogger(SlidingWindowTopology.class);

    /*
     * Computes tumbling window average
     */
    private static class TumblingWindowAvgBolt extends BaseWindowedBolt {
        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(TupleWindow inputWindow) {
            int sum = 0;
            List<Tuple> tuplesInWindow = inputWindow.get();
            LOG.debug("Events in current window: " + tuplesInWindow.size());
            if (tuplesInWindow.size() > 0) {
                /*
                * Since this is a tumbling window calculation,
                * we use all the tuples in the window to compute the avg.
                */
                for (Tuple tuple : tuplesInWindow) {
                    sum += (int) tuple.getValue(0);
                }
                collector.emit(new Values(sum / tuplesInWindow.size()));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("avg"));
        }
    }


    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("integer", new RandomIntegerSpout(), 1);
        
        builder.setBolt("slidingsum", new SlidingWindowSumBolt().withWindow(new Count(10), new Count(5)), 1)
                .shuffleGrouping("integer");
        
        builder.setBolt("tumblingavg", new TumblingWindowAvgBolt().withTumblingWindow(new Count(3)), 1)
                .shuffleGrouping("slidingsum");
        
        builder.setBolt("printer", new PrinterBolt(), 1).shuffleGrouping("tumblingavg");
        
        Config conf = new Config();
        conf.setDebug(false);
        if (args != null && args.length > 0) {
            conf.setNumWorkers(1);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("sliding_window", conf, builder.createTopology());
            Utils.sleep(20000);
            cluster.killTopology("sliding_window");
            cluster.shutdown();
            System.exit(1);
        }
    }
}
