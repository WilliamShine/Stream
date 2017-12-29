package com.YunTu.TopoLine;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class Changer {

	/*@Test
	public void test1() {
		String results = "update DTstorm_result set dwtvs_count_num=? where dwtvs_task=? and dwtvs_logo=? and dwtvs_name=? and dwtvs_date_year=? and dwtvs_date_month=? and dwtvs_date_day=? and dwtvs_date_hour=? and dwtvs_type=?";
		String inserChanger = exchanger(results);
		System.out.println(inserChanger);
	}
//insert into DTstorm_result(dwtvs_task,dwtvs_logo,dwtvs_name,dwtvs_date_year,dwtvs_date_month,dwtvs_date_day,dwtvs_date_hour,dwtvs_count_num,
	//dwtvs_type) values (?,?,?,?,?,?,?,?,?)"
	private String exchanger(String results) {
		String[] result = results.split(" ", -1);
		String tableName = result[1];
		List<String> list = new ArrayList<String>();
		for (int i = 0; i < results.length(); i++) {
			if (results.charAt(i)=='?') {
				int s = i;
				while (results.charAt(s--)!= ' ') {
					if (results.charAt(s-1)== ' ') {
						list.add(results.substring(s, i-1));
					}
				}
			}
		}
		String newResults = "insert into "+tableName+"(";
		String num = "";
		for (String string : list) {
			newResults+=string+",";
			num+="?,";
		}
		newResults=newResults.substring(0, newResults.length()-1)+") values ("+num.substring(0, num.length()-1)+")";
		
		return newResults;
	}*/
}
