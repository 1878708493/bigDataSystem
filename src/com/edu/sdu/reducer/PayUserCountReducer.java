package com.edu.sdu.reducer;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 支付用户人数的reducer
 * @author 王宁
 *
 */
public class PayUserCountReducer extends Reducer<Text, FloatWritable, Text, Text> {

	public void reduce(Text _key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
		int size = 0;
		// process values
		for (FloatWritable val : values) {
			if(val.get() > 0.00){
				size++;
			}
		}
		context.write(_key, new Text(size + ""));
	}

}
