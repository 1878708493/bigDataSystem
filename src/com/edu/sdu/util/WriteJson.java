package com.edu.sdu.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import com.google.gson.*;

public class WriteJson {
	
	public static void doWriteFile(String inputPath, String outputPath) throws Exception {

		/**
		 * 输入文件、输出文件路径，根据情况改变路径
		 * */
/*		String inputPath = "/home/hduser/Downloads/json/new.txt";
		String outputPath = "/home/hduser/Downloads/json/new02.txt";*/

		File file = new File(inputPath);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line = null;
		JsonObject jsonObject = null;
		JsonObject app_jsonObject;
		JsonArray jsonArray = new JsonArray();
		JsonArray app_jsonArray = new JsonArray();
		

		/**
		 * 获取文件第一个APPKEY
		 */
		BufferedReader readerfirst = new BufferedReader(new FileReader(file));
		String[] filesFirst = readerfirst.readLine().split("\t");
		String firstline = null;
		String app_keyfirst = filesFirst[0];
		
		String date = filesFirst[3];
		
		/**
		 * 获取一个APPKEY下面所拥有的value数量
		 * */
		int num = 1;
		while ((firstline = readerfirst.readLine()) != null) {
			String[] newfilesFirst = firstline.split("\t");
			String m_app_key = newfilesFirst[0];
			if (!app_keyfirst.equals(m_app_key)) {
				break;
			}
			num++;
		}

		String app_key = null;
		int count = 0;
		
		/**
		 * 循环读取文件，构建JSON
		 * */
		while ((line = reader.readLine()) != null) {

			String[] files = line.split("\t");

			app_key = files[0];

			jsonObject = new JsonObject();
			jsonObject.addProperty("value", files[1]);
			jsonObject.addProperty("count", files[2]);
			jsonArray.add(jsonObject);

			if (count == (num - 1) && app_key.equals(app_keyfirst)) {
				app_jsonObject = new JsonObject();
				app_jsonObject.addProperty("app_key", app_key);
				app_jsonObject.add("data", jsonArray);
				app_jsonArray.add(app_jsonObject);
				jsonArray = new JsonArray();
				count = 0;
			}

			if (count == num && !app_key.equals(app_keyfirst)) {
				app_jsonObject = new JsonObject();
				app_jsonObject.addProperty("app_key", app_key);
				app_jsonObject.add("data", jsonArray);
				app_jsonArray.add(app_jsonObject);
				jsonArray = new JsonArray();
				count = 0;
			}

			count++;

		}

		JsonObject finalJSON = new JsonObject();
		finalJSON.addProperty("date", date);
		finalJSON.add("data", app_jsonArray);
		System.out.println(finalJSON);

		writeFile(outputPath, finalJSON.toString());

	}

	/**
	 * 读取TXT文件，以字符串返回
	 */
	public static String ReadFile(String path) {
		File file = new File(path);
		BufferedReader reader = null;
		String laststr = "";
		try {
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			while ((tempString = reader.readLine()) != null) {
				laststr = laststr + tempString;
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}
		return laststr;
	}

	/**
	 * 将JSON格式信息写入文件（需要将JSON信息转化成string类型）
	 */
	public static void writeFile(String filePath, String sets) throws IOException {
		FileWriter fw = new FileWriter(filePath);
		PrintWriter out = new PrintWriter(fw);
		out.write(sets);
		out.println();
		fw.close();
		out.close();
	}

}
