package com.YunTu.MStorm.simpleKafka;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;

public class KafkaScheme implements Scheme {
	public static final String STRING_SCHEME_KEY = "myscheme";
	
	public List<Object> deserialize(byte[] bytes) {
		List<Object> list = new ArrayList<Object>();//和这个很关键
		try {
			list.add(new String(bytes, "UTF-8"));
			list.add(new String(bytes, "GBK"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return list;
	}
	
	public Fields getOutputFields() {
		
		return new Fields("myscheme","message");//这个很关键
	}

}
