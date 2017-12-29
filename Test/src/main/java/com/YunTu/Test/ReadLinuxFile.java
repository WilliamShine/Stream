package com.YunTu.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class ReadLinuxFile {

	public static void main(String[] args) {
		try {//OriginalTopology.class.getClassLoader().getResourceAsStream(configPath)
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(args[0])));//new FileReader(new File(args[0]))
			while (bufferedReader.ready()) {
				String line = bufferedReader.readLine();
				System.out.println(line);
			}
			bufferedReader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
