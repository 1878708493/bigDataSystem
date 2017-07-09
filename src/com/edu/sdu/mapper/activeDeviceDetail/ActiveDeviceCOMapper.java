package com.edu.sdu.mapper.activeDeviceDetail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.sdu.edu.bean.PlayerDeviceDetailBean;

public class ActiveDeviceCOMapper extends Mapper<LongWritable, Text, PlayerDeviceDetailBean, Text> {

	Map<String, String> map = new HashMap<>();
	
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String line = ivalue.toString();
		String val[] = line.split("\\s+");
		
		String coType = val[13] + " " + val[14]; // 运营商
		String app_key = val[0];
		String time = val[6];
		String device = val[3];
			
		if (!map.containsKey(app_key + " " + device + " " + coType)) {
			map.put(app_key + " " + device + " " + coType, "");
			context.write(new PlayerDeviceDetailBean(app_key, coType), new Text(time));
		}
	}
}