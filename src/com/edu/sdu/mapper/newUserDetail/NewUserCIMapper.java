package com.edu.sdu.mapper.newUserDetail;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.edu.sdu.bean.PlayerDeviceDetailBean;

public class NewUserCIMapper extends Mapper<LongWritable, Text, PlayerDeviceDetailBean, Text> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String line = ivalue.toString();
		String val[] = line.split("\\s+");
		
		String ciType = val[16]; // 联网方式
		String app_key = val[0];
		String time = val[6];
		String state = val[5];
		
		if(state.equals("0"))
			context.write(new PlayerDeviceDetailBean(app_key, ciType), new Text(time));
	}
}
