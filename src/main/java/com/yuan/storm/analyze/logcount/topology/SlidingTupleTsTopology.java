package com.yuan.storm.analyze.logcount.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.utils.Utils;

import com.yuan.storm.analyze.logcount.bolt.PrinterBolt;
import com.yuan.storm.analyze.logcount.bolt.SlidingWindowSumBolt;


import com.yuan.storm.analyze.logcount.spout.RandomIntegerSpout;

import java.util.concurrent.TimeUnit;

import static org.apache.storm.topology.base.BaseWindowedBolt.Duration;

/**
 * Windowing based on tuple timestamp (e.g. the time when tuple is generated
 * rather than when its processed).
 */
public class SlidingTupleTsTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        BaseWindowedBolt bolt = new SlidingWindowSumBolt()
                .withWindow(new Duration(10, TimeUnit.SECONDS), new Duration(10, TimeUnit.SECONDS))
                .withTimestampField("ts")
                .withWatermarkInterval(new Duration(1, TimeUnit.SECONDS))
                .withLag(new Duration(2, TimeUnit.SECONDS));
        
        builder.setSpout("integer", new RandomIntegerSpout(), 1);
        builder.setBolt("slidingsum", bolt, 1).shuffleGrouping("integer");
        builder.setBolt("printer", new PrinterBolt(), 1).shuffleGrouping("slidingsum");
        Config conf = new Config();
        conf.setDebug(false);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(1);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(40000);
            cluster.killTopology("test");
            cluster.shutdown();
            System.exit(1);
        }
    }
}