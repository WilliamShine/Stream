package com.YunTu.LineStream.resource;

import java.util.HashMap;
import java.util.Map;

/**
 * 资源池
 * 
 * @author 84031
 *
 */
public class ShareResource {

	// 用于存放字节资源
	private static final Map<String, Long> map = new HashMap<String, Long>();//单维度map。	 Map<String,Map<String,Long>>
	private static boolean updateFlag = true;// 更新标识，第一次必须加装

	/**
	 * @description 通过class的全路径进行查找对应的字节数组
	 * @param clazName
	 *            class全路径
	 * @return 返回一个字节数组
	 */
	public static Long findLongValue(String id) {
		Long by = null;
		if (map != null)
			by = map.get(id);
		return by;
	}
	
	public static void putValue(String id,Long value) {
		if (map.get(id)!=null) {
			if (value - map.get(id) < 0 || value < 0) {
				System.out.println("破环资源池数据，脏数据+id"+id);
			} 
		}
		map.put(id, value);
//		setUpdateFlag(true);
	}

	/*public static void setMap(Map<String, Long> map) {
		ShareResource.map = map;
	}*/

	public static Map<String, Long> getMap() {
		return map;
	}
	
	public static void setUpdateFlag(boolean updateFlag) {
		ShareResource.updateFlag = updateFlag;
	}
	
	public static boolean isUpdateFlag() {
		return updateFlag;
	}
}
