package com.yuan.storm.analyze.logcount.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;


public class KafkaWordSplitter extends BaseRichBolt {

	private static final Logger LOG = LoggerFactory
			.getLogger(KafkaWordSplitter.class);
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private String projectName = "projectName";
	private String clazzName = "clazz";
	private String methodName = "method";
	private String timestamp = null;
	private long ts = 0l;
	
	

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
//		jedis = new Jedis("172.28.29.151", 6379);
//		jedis.del("s_server");
		
	}

	@Override
	public void execute(Tuple input) {
		String line = input.getString(0);
		String msgID = null;
		
//		LOG.info("RECV[kafka -> splitter] " + line);
//		System.out.println("KafkaWordSplitterBoltRecive: " + line);
		
		JSONObject jsonObj = new JSONObject(line);
		
		//JSON 格式化
		try{		
//			timestamp = jsonObj.getString("timestamp");
			ts = (long) jsonObj.get("date");
//			ts = Long.parseLong(timestamp);
		} catch(JSONException ex){
			System.out.println("JSONException" + ex.toString());
		}
		
		try{
//			msgID = jsonObj.getString("msgID");
			msgID = jsonObj.getString("id");
		}catch(JSONException ex){
			System.out.println("JSONException" + ex.toString());
		}
		
		try{		
			String value = jsonObj.getString(projectName);
			this.collector.emit("projectStreamID", new Values(value, ts, msgID));
//			System.out.println("WordSplitterBolt: value " + value + " ts: " + ts + " msgID:" + msgID);
		} catch(JSONException ex){
			System.out.println("JSONException" + ex.toString());
		}
				
		try {
			String classValue = null;
			classValue = jsonObj.getString(clazzName).split(" ")[jsonObj.getString(clazzName).split(" ").length - 1];
			this.collector.emit("classStreamID", new Values(classValue, ts, msgID));
		}catch(JSONException ex){
			System.out.println("JSONException" + ex.toString());
		}
		
		try {
			String methodValue = null;
			methodValue = jsonObj.getString(methodName);
			this.collector.emit("methodStreamID", new Values(methodValue, ts, msgID));
		}catch(JSONException ex){
			System.out.println("JSONException" + ex.toString());
		}
		
		try {
			String outputMsg = null;
			outputMsg = jsonObj.getString("outputInfo");
			JSONObject outputObj = new JSONObject(outputMsg);
			String resultCode = outputObj.getString("returnCode");
			this.collector.emit("resultCodeID", new Values(resultCode, ts, msgID));
		}catch(JSONException ex){
			System.out.println("JSONException" + ex.toString());
		}
		
		ts = 0l;
		
		collector.ack(input);
//		Utils.sleep(700);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("projectStreamID", new Fields("projectName", "ts", "msgID"));
		declarer.declareStream("methodStreamID", new Fields("methodName", "ts", "msgID"));
		declarer.declareStream("classStreamID", new Fields("className", "ts", "msgID"));
		declarer.declareStream("resultCodeID", new Fields("resultCode", "ts", "msgID"));
	}

}
