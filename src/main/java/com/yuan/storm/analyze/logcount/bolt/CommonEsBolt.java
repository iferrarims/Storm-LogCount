package com.yuan.storm.analyze.logcount.bolt;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonEsBolt extends BaseRichBolt {
	private static final long serialVersionUID = 2021198015295864975L;
	private static final Logger LOG = LoggerFactory.getLogger(CommonEsBolt.class);
	private OutputCollector collector;
	private Map<String, Object> counts = new HashMap<String, Object>();
	private Client client;
	private String ip = "172.28.20.100";

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		 Settings settings = Settings.settingsBuilder().put("cluster.name", "log_count").build();
		 try {
			client = TransportClient.builder().settings(settings).build().addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ip), 9300));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		counts = (Map<String, Object>) input.getValueByField("count");
		
		/* 产生索引 */
		Date now = new Date();
		DateFormat df = new SimpleDateFormat("yyyy.MM.dd");
		String logIndex = "logcount-" + df.format(now);
		
		counts.put("@timestamp", now);

    	try {
            IndexResponse response = client.prepareIndex(logIndex, "info")
                    .setSource(counts)
                    .execute()
                    .actionGet();
        } catch (Exception e) {
            e.printStackTrace();
        }
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
