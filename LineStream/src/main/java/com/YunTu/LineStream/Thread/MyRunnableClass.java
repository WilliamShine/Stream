package com.YunTu.LineStream.Thread;

import java.util.Map;

public class MyRunnableClass implements Runnable{
	Map map;

	public MyRunnableClass(Map map) {
		this.map = map;
	}
	@Override
	public void run() {
		while (true) {
			System.out.println("Thread"+map.size());
		}
	}

}
