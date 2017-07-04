package com.edu.sdu.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 每日新增设备的mapper
 * @author 王宁
 *
 */
public class DailyNewDeviceMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String line = ivalue.toString();
		String[] val = line.split("\\s+");
		
		String app_key = val[1];
		String state = val[6];
		
		if(state.equals("4"))
			context.write(new Text(app_key), new Text(state));
	}

}
