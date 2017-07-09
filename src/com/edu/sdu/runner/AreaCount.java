package com.edu.sdu.runner;

import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.edu.sdu.mapper.AreaCountMapper;
import com.edu.sdu.mapper.DailyNewDeviceMapper;
import com.edu.sdu.mapper.RemainDeviceMapper;
import com.edu.sdu.reducer.AreaCountReducer;
import com.edu.sdu.reducer.DailyNewDeviceReducer;
import com.edu.sdu.reducer.RemainDeviceReducer;
import com.edu.sdu.util.Database;
import com.sdu.edu.bean.AreaOrderBean;
import com.sdu.edu.bean.RemainOprBean;
import com.sdu.edu.bean.Sysmbol;
import com.sdu.edu.bean.TimeValueBean;

public class AreaCount {
	
	public static void main(String[] args) throws Exception {
		Sysmbol.startDay = "2017-05-02.txt";
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(AreaCount.class);
		job.setOutputKeyClass(AreaOrderBean.class);
	    job.setOutputValueClass(Text.class);
		job.setMapperClass(AreaCountMapper.class);
		job.setReducerClass(AreaCountReducer.class);
		//FileInputFormat.setInputPaths(job, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileInputFormat.setInputPaths(job, new Path("/usr/local/hadoop/file/usr/" + Sysmbol.startDay));
		
		FileSystem fs2 = FileSystem.get(conf);
		Path op2 = new Path("/usr/local/hadoop/file/AreaCount");
		if (fs2.exists(op2)) {
			fs2.delete(op2, true);
			System.out.println("存在此输出路径，已删除！！！");
		}
		FileOutputFormat.setOutputPath(job, op2);
		job.waitForCompletion(true);
		
		/*向数据库写数据操作*/
		Database database = Database.getInstance();
		FileReader file = new FileReader("/usr/local/hadoop/file/AreaCount/part-r-00000");
		BufferedReader bReader = new BufferedReader(file);
		String str = null;
		int index = 0;
		boolean flag = false;
		while ((str = bReader.readLine()) != null) {
			String val[] = str.split("\\s+");
			String app_key = val[0];
			String city = val[1];
			String usercount = val[3];
			String time = val[4];
			
			str = bReader.readLine();
			val = str.split("\\s+");
			String deviceCount = val[3];
			
			flag = database.updateArea(app_key, city, deviceCount, usercount, time);
		}
		System.out.println(flag);
	}
}