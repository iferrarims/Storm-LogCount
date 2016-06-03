package com.yuan.storm.analyze.logcount.topology;

import java.util.Arrays;
import java.util.UUID;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.yuan.storm.analyze.logcount.bolt.KafkaWordSplitter;
import com.yuan.storm.analyze.logcount.bolt.WordCounter;

public class LogcountTopology {
	public static void main (String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, InterruptedException {
//		String zks = "172.28.18.209:2181,172.28.18.210:2181,172.28.18.211:2181,172.28.18.219:2181,172.28.18.220:2181";
		String zks = "172.28.20.103:2181,172.28.20.104:2181,172.28.20.105:2181";
		String topic = "test18";
		String zkRoot = "/storm"; // default zookeeper root configuration for
									// storm
		String id = LogcountTopology.class.getSimpleName();
		
		BrokerHosts brokerHosts = new ZkHosts(zks);
		SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, UUID.randomUUID().toString());
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConf.zkServers = Arrays.asList(new String[] {"172.28.20.103","172.28.20.104","172.28.20.105"});
//		spoutConf.zkServers = Arrays.asList(new String[] {"172.28.18.209","172.28.18.210","172.28.18.211","172.28.18.219","172.28.18.220"});
		spoutConf.zkPort = 2181;
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), 5); // Kafka我们创建了一个5分区的Topic，这里并行度设置为5
		builder.setBolt("word-splitter", new KafkaWordSplitter())
				.shuffleGrouping("kafka-reader");
//		builder.setBolt("word-counter", new WordCounter()).fieldsGrouping(
//				"word-splitter", new Fields("word"));

		Config conf = new Config();
		
		if ((args != null) && (args.length > 0)) {
		    conf.setNumWorkers(2);
		    conf.setMaxSpoutPending(5000);
		    StormSubmitter.submitTopology("log_analyze_topology", conf, builder.createTopology());
		} else {
		    LocalCluster cluster = new LocalCluster();
		    cluster.submitTopology("log_analyze_topology", conf, builder.createTopology());
		    Thread.sleep(30*1000);
			cluster.killTopology("log_analyze_topology");
			cluster.shutdown();
			System.exit(1);
		}
	}
}
