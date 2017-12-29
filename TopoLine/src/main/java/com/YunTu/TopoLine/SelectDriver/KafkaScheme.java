package com.YunTu.TopoLine.SelectDriver;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;


public class KafkaScheme implements Scheme {
	public static final String STRING_SCHEME_KEY = "myscheme";
	
	public Fields getOutputFields() {
		
		return new Fields("myscheme","message");//这个很关键
	}

	public List<Object> deserialize(ByteBuffer buffer) {
		List<Object> list = new ArrayList<Object>();//和这个很关键
		try {
			list.add(Charset.forName("UTF-8").newDecoder().decode(buffer.asReadOnlyBuffer()).toString());
			list.add(Charset.forName("GBK").newDecoder().decode(buffer.asReadOnlyBuffer()).toString());
		}  catch (CharacterCodingException e) {
			e.printStackTrace();
		}
		return list;
	}

}
