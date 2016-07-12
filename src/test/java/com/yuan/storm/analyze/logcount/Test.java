package com.yuan.storm.analyze.logcount;

import org.json.JSONObject;

public class Test {
	
	public static void main(String[] args) {
//		System.out.println("class123".split(" ")["class123".split(" ").length-1]);
		
		String msg = "{\"outInfo\":\"{\\\"aaa\\\":\\\"bbb\\\"}\"}";
		
		JSONObject jsonObj = new JSONObject(msg);
		String outInfo = (String) jsonObj.get("outInfo");
		
		System.out.println("outinfo " + outInfo);
		
		JSONObject info = new JSONObject(outInfo);
		
		String aaa = (String) info.get("aaa");
		
		System.out.println("aaa : " + aaa);

	}

}
