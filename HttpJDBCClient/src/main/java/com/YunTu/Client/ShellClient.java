package com.YunTu.Client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class ShellClient {

	public static void main(String[] args) {
		//1.根据josn写配置文件。传入一个json，对应的放进配置中。模板到配置
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File("/home/shine/profile")));
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File("/home/shine/profile_client")));
			while (br.ready()) {
				String line = br.readLine();
				String name = line.split("=",-1)[0];
				switch (name) {
				
				case "kafka_spout1_topicname":
					line="kafka_spout1_topicname="+"";
					break;
				case "t_ebiz_comment.release_date":
					line="t_ebiz_comment.release_date="+"";
					break;
				case "t_ebiz_comment.hit_tag":
					line="t_ebiz_comment.hit_tag="+"";
					break;
				case "DSSLresult.dwtvs_type":
					line="DSSLresult.dwtvs_type="+"";
					break;
					
				case "kafka_spout2_topicname":
					line="kafka_spout2_topicname="+"";
					break;
				case "t_data.release_date":
					line="t_data.release_date="+"";
					break;
				case "t_data.url_crc":
					line="t_data.url_crc="+"";
					break;
				case "t_data.rel_type":
					line="t_data.rel_type="+"";
					break;
				case "t_label.url_crc":
					line="t_data.rel_type="+"";
					break;
				case "t_label.hit_tag":
					line="t_data.rel_type="+"";
					break;
				case "FDSZTSLresult.dwtvs_type":
					line="FDSZTSLresult.dwtvs_type="+"";
					break;//fdsztsl_t_label_hdfsip,fdsztsl_T_label_path
					
				case "kafka_spout3_topicname":
					line="kafka_spout3_topicname="+"";
					break;
				case "t_comment.release_date":
					line="t_comment.release_date="+"";
					break;
				case "t_comment.c_url_crc":
					line="t_comment.c_url_crc="+"";
					break;
				case "FDSPLSLresult.dwtvs_type":
					line="FDSPLSLresult.dwtvs_type="+"";//t_data.url_crc/rel_type//t_label.url_crc/hit_tag
					break;
				default:
					break;
				}
				bw.write(line);
				bw.flush();
			}
			if (bw!=null) {
				bw.close();
			}
			if (br!=null) {
				br.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		//2.杀Topology进程
		//storm kill OriginalTopology -0
		//3.开Topology进程
		//·r.OriginalTopology /home/shine/profile_client
		
	}
}
