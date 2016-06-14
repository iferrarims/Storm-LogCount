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
import org.elasticsearch.common.transport.TransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Write2EsBolt extends BaseRichBolt {
	
	private static final Logger LOG = LoggerFactory.getLogger(Write2EsBolt.class);
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private Map<String, Integer> counts = new HashMap<String, Integer>();
	private long ts = 0;
	private String name = null;
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple input) {
		counts = (Map<String, Integer>) input.getValueByField("sumMap");
		ts = (long) input.getValueByField("ts");
		name = (String) input.getValueByField("name");
		
		/* 产生索引 */
		Date now = new Date();
		DateFormat df = new SimpleDateFormat("yyyy.MM.dd");
        String logIndex = "logcount-" + df.format(now);
		
		Iterator iter = counts.entrySet().iterator();
		while (iter.hasNext()) {
        	Map.Entry<String, Integer> entry = (Entry<String, Integer>) iter.next();
        	Map<String, Object> sumCount = new HashMap<String, Object>();
        	sumCount.put("category_name", name);
//        	sumCount.put("timestamp", new Date(ts));
        	sumCount.put("timestamp", new Date(ts));
        	sumCount.put("item_name", entry.getKey());
        	sumCount.put("total_count", entry.getValue());
        	sumCount.put("@timestamp", new Date());
        	
        	try {
                IndexResponse response = client.prepareIndex(logIndex, "info")
                        .setSource(sumCount)
                        .execute()
                        .actionGet();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }	
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
