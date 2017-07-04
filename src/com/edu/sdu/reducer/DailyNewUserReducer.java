package com.edu.sdu.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DailyNewUserReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// process values
		int size = 0;
		
		for (Text val : values) {
			size++;
		}
		context.write(_key, new Text(size + ""));
	}

}
