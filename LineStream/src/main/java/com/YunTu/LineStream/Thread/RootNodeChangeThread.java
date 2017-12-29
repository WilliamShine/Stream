package com.YunTu.LineStream.Thread;

import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

import org.apache.storm.task.OutputCollector;

import com.YunTu.LineStream.resource.ShareResource;

public class RootNodeChangeThread implements Runnable {
	Map map ;
	Long pv;
	Long uv;
	OutputCollector collector;
	
	long _lastUpdateMs = System.currentTimeMillis();
	long _stateUpdateIntervalMs = 2000;
	long nowTime ;
	long diffWithNow ;

	int _currPartitionIndex = 0;

	public RootNodeChangeThread(Map map,OutputCollector collector) {
		this.map = map;
	}
	@Override
	public void run() {
		 while(true) {
			 nowTime = System.currentTimeMillis();
			diffWithNow = nowTime - _lastUpdateMs;
				if (diffWithNow > _stateUpdateIntervalMs || diffWithNow < 0) {
//					if () {//非实时，准实时策略ShareResource.isUpdateFlag()
					//TODO
					int pv = 0;
					int uv = 0;
					for (Entry<String, Long> e : ShareResource.getMap().entrySet()) {
						uv++;
						pv += e.getValue();
					}
					System.out.println(">>>>>>>>>> 产生随机的 uuid string,'uuidStr'===>"+map.size());
//					System.out.println("PV:"+pv+"----UV:"+uv);
						_lastUpdateMs = nowTime;// 更新策略_lastUpdateMs=System.currentTimeMillis();
//						ShareResource.setUpdateFlag(false);//准实时策略，存在先后差距
//					}
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
					//Thread.currentThread().yield();  
				}
	            
             /*try {
                 Thread.sleep(2000);
             } catch (InterruptedException e) {
                 //TODO
             }*/
             
            /* String uuidStr=UUID.randomUUID().toString();    
             
             System.out.println(">>>>>>>>>> 产生随机的 uuid string,'uuidStr'===>"+map.size());
//             collector.emit(map.size())
*/             
         }
         

	}

}
