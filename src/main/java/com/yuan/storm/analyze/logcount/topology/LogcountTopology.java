package com.yuan.storm.analyze.logcount.topology;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaConfig;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.trident.spout.RichSpoutBatchExecutor;
import org.apache.storm.tuple.Fields;

import com.yuan.storm.analyze.logcount.bolt.ClassCountBolt;
import com.yuan.storm.analyze.logcount.bolt.CommonEsBolt;
import com.yuan.storm.analyze.logcount.bolt.KafkaWordSplitter;
import com.yuan.storm.analyze.logcount.bolt.MethodCountBolt;
import com.yuan.storm.analyze.logcount.bolt.PrinterBolt;
import com.yuan.storm.analyze.logcount.bolt.ProjectCountBolt;
import com.yuan.storm.analyze.logcount.bolt.ResultCodeSummaryBolt;
import com.yuan.storm.analyze.logcount.bolt.SlidingWindowSumBolt;
import com.yuan.storm.analyze.logcount.bolt.WordCounter;
import com.yuan.storm.analyze.logcount.bolt.Write2EsBolt;

public class LogcountTopology {
	public static void main (String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, InterruptedException {
		String zks = "172.28.18.209:2181,172.28.18.210:2181,172.28.18.211:2181,172.28.18.219:2181,172.28.18.220:2181";
//		String zks = "172.28.20.103:2181,172.28.20.104:2181,172.28.20.105:2181";
		String topic = "logService";
//		String topic = "test30";
		String zkRoot = "/storm"; // default zookeeper root configuration for
									// storm
		String id = LogcountTopology.class.getSimpleName();
		String uuid = UUID.randomUUID().toString();
		
		BrokerHosts brokerHosts = new ZkHosts(zks);
		SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, "sumcount3");
//		SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, "logcountID");
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
//		spoutConf.zkServers = Arrays.asList(new String[] {"172.28.20.103","172.28.20.104","172.28.20.105"});
//		spoutConf.zkServers = Arrays.asList(new String[] {"172.28.20.103","172.28.20.104","172.28.20.105"});
		spoutConf.zkServers = Arrays.asList(new String[] {"172.28.18.209","172.28.18.210","172.28.18.211","172.28.18.219","172.28.18.220"});
//		spoutConf.zkServers = Arrays.asList(new String[] {"172.28.18.209","172.28.18.210","172.28.18.211","172.28.18.219","172.28.18.220"});
		spoutConf.zkPort = 2181;
		spoutConf.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		
//		KafkaConfig kafkaConf = new KafkaConfig(brokerHosts, uuid);
//		kafkaConf.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		
		BaseWindowedBolt projectBolt = new ProjectCountBolt()
        	.withWindow(new Duration(10, TimeUnit.SECONDS), new Duration(10, TimeUnit.SECONDS));
//        	.withTimestampField("ts")
//        	.withWatermarkInterval(new Duration(1, TimeUnit.SECONDS))
//        	.withLag(new Duration(5, TimeUnit.SECONDS));
		
		BaseWindowedBolt classBolt = new ClassCountBolt()
	    	.withWindow(new Duration(10, TimeUnit.SECONDS), new Duration(10, TimeUnit.SECONDS));
//	    	.withTimestampField("ts")
//	    	.withWatermarkInterval(new Duration(1, TimeUnit.SECONDS))
//	    	.withLag(new Duration(5, TimeUnit.SECONDS));
		
		BaseWindowedBolt methodBolt = new MethodCountBolt()
	    	.withWindow(new Duration(10, TimeUnit.SECONDS), new Duration(10, TimeUnit.SECONDS));
//	    	.withTimestampField("ts")
//	    	.withWatermarkInterval(new Duration(1, TimeUnit.SECONDS))
//	    	.withLag(new Duration(5, TimeUnit.SECONDS));
		
		BaseWindowedBolt resultCodeBolt = new ResultCodeSummaryBolt()
    		.withWindow(new Duration(10, TimeUnit.SECONDS), new Duration(10, TimeUnit.SECONDS));
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), 6);
		builder.setBolt("word-splitter", new KafkaWordSplitter(), 6)
				.shuffleGrouping("kafka-reader");
		/* 根据streamID将tuple分发到不同的bolt */
		builder.setBolt("project-count", projectBolt, 2).shuffleGrouping("word-splitter", "projectStreamID");
		builder.setBolt("class-count", classBolt, 2).shuffleGrouping("word-splitter", "classStreamID");
		builder.setBolt("method-count", methodBolt, 2).shuffleGrouping("word-splitter", "methodStreamID");
		builder.setBolt("resultcode-count", resultCodeBolt, 2).shuffleGrouping("word-splitter", "resultCodeID");
		
		builder.setBolt("emit2es", new Write2EsBolt(), 6).shuffleGrouping("project-count").shuffleGrouping("class-count").shuffleGrouping("method-count");
		builder.setBolt("common-es", new CommonEsBolt(), 2).shuffleGrouping("resultcode-count");
		
//		builder.setBolt().shuffleGrouping("project-count");
//		builder.setBolt("printer1", new PrinterBolt(), 1).shuffleGrouping("word-splitter", "methodStreamID");
//		builder.setBolt("printer2", new PrinterBolt(), 1).shuffleGrouping("word-splitter", "clazzStreamID");


		Config conf = new Config();
		
		if ((args != null) && (args.length > 0)) {
		    conf.setNumWorkers(3);
		    conf.setMaxSpoutPending(5000);
		    conf.put(RichSpoutBatchExecutor.MAX_BATCH_SIZE_CONF, 64 * 1024);
		    StormSubmitter.submitTopology("LogcountTopology", conf, builder.createTopology());
		} else {
		    LocalCluster cluster = new LocalCluster();
		    cluster.submitTopology("LogcountTopology", conf, builder.createTopology());
		    Thread.sleep(140*1000);
			cluster.killTopology("LogcountTopology");
			cluster.shutdown();
			System.exit(1);
		}
	}
}
