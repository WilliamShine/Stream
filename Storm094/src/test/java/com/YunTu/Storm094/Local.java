package com.YunTu.Storm094;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.junit.Test;


public class Local {
	
	@Test
	public void readTest() {
		 try {
			 BufferedReader br = new BufferedReader(new FileReader("//C:\\Users\\84031\\Desktop\\shine\\t_data_1407714309205630_60410_5934_1\\t_dataaa"));
			 while (br.ready()) {
					String line=br.readLine();
					System.out.println(line);
				}
		 } catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
