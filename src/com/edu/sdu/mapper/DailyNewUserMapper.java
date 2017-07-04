package com.edu.sdu.mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.edu.sdu.main.DailyNewUser;

public class DailyNewUserMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String line = ivalue.toString();
		String[] val = line.split("\\s+");
		
		String app_key = val[1];
		String behavior = val[6];
		String userId = val[3];	// 用户id
		
		if(behavior.equals("0"))
			context.write(new Text(app_key), new Text(userId));	// appkey, userid
	}
}
