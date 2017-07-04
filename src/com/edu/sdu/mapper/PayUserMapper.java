package com.edu.sdu.mapper;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 每天每个app的用户支付人物以及支付总金额的mapper
 * @author 王宁
 *
 */
public class PayUserMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String line = ivalue.toString();
		String val[] = line.split("\\s+");
		
		String app_key = val[1];
		float cost = Float.parseFloat(val[11]);
		FloatWritable money = new FloatWritable(cost);
		context.write(new Text(app_key), money);
	}

}
