package com.edu.sdu.reducer;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 获取新用户数量的reducer
 * @author 王宁
 *
 */
public class NewUserReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// process values
		ArrayList<Text> list = new ArrayList<>();
		for (Text val : values) {
			list.add(val);
		}
		if(list.size() == 1){
			if(list.get(0).toString().equals("#")){
				context.write(_key, new Text(""));
			}
		}
	}

}
