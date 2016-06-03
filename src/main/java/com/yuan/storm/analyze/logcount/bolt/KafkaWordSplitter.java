package com.yuan.storm.analyze.logcount.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
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
	private String clazzName = "clazzName";
	private String methodName = "methodName";
	private Jedis jedis = null;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		jedis = new Jedis("172.28.29.151", 6379);
	}

	@Override
	public void execute(Tuple input) {
		String line = input.getString(0);
		LOG.info("RECV[kafka -> splitter] " + line);
		
		//JSON 格式化
		try{
			
			JSONObject jsonObj = new JSONObject(line);
			String value = jsonObj.getString(projectName);
			if(value!=null || !value.isEmpty()){

				this.jedis.zincrby("s_server", 1, value);				
				try {
					String clazzValue = null;
					clazzValue = jsonObj.getString(clazzName);
					if(clazzValue!=null || !clazzValue.isEmpty()){
						String member = value + "-" + clazzValue;
	
						this.jedis.zincrby("s_clazz", 1, member);
						System.out.println(clazzName+" 出现的次数为： " + this.jedis.zscore("s_clazz", member));
					}
					String methodValue = jsonObj.getString(methodName);
					if(methodValue != null || !methodValue.isEmpty()){
						String methodMember = value + "-" + methodValue;
	
						this.jedis.zincrby("s_method", 1, methodMember);
						System.out.println(clazzName + " 出现的次数为： " + this.jedis.zscore("s_method", methodMember));
					}
				}catch(JSONException ex){
					
				}
			}
			
			System.out.println(projectName+" 出现的次数为： " + this.jedis.zscore("s_server", value));
		}catch(Exception ex){
			LOG.error("", ex);
			this.collector.fail(input);//告知spout 失败
		}
		
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}

}
