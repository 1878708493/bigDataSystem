package com.edu.sdu.reducer;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PayMoneyReducer extends Reducer<Text, FloatWritable, Text, Text> {

	public void reduce(Text _key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
		float money = 0.00f;
		// process values
		for (FloatWritable val : values) {
			if(val.get() > 0.00){
				money += val.get();
			}
		}
		context.write(_key, new Text(money + ""));
	}

}
