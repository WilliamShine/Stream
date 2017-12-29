package com.YunTu.MavenStormKafka;

import java.io.UnsupportedEncodingException;
import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MessageScheme implements Scheme {

	public List<Object> deserialize(byte[] arg0) {
		try {
			String msg = new String(arg0, "UTF-8");
			return new Values(msg);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return null;
	}

	public Fields getOutputFields() {
		return new Fields("msg");
	}

}
