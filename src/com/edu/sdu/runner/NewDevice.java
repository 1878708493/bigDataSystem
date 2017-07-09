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

import com.edu.sdu.mapper.NewDeviceMapper;
import com.edu.sdu.reducer.NewDeviceReducer;
import com.edu.sdu.util.Database;
import com.sdu.edu.bean.TimeValueBean;

public class NewDevice {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(NewDevice.class);

		job.setMapperClass(NewDeviceMapper.class);
		job.setReducerClass(NewDeviceReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TimeValueBean.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TimeValueBean.class);

		FileInputFormat.addInputPath(job, new Path("/usr/local/hadoop/file/nusr/1.txt"));
		
		FileSystem fs2 = FileSystem.get(conf);
		Path op2 = new Path("/usr/local/hadoop/file/NewDevice");
		if (fs2.exists(op2)) {
			fs2.delete(op2, true);
			System.out.println("存在此输出路径，已删除！！！");
		}
		FileOutputFormat.setOutputPath(job, op2);
		/* job.submit(); */
		job.waitForCompletion(true);
		
		/*向数据库写数据操作*/
		Database database = Database.getInstance();
		FileReader file = new FileReader("/usr/local/hadoop/file/NewDevice/part-r-00000");
		BufferedReader bReader = new BufferedReader(file);
		String str = null;
		int index = 0;
		String app_key = "";
		String one_fours = ""; String five_tens = ""; String eleven_thirtys = ""; 
		String thirty_sixtys = ""; String one_threem = ""; String four_tenm = ""; String eleven_thirtym = "";
		String thirty_sixtym = ""; String sixtym_more = "";
		String time = null;
		while ((str = bReader.readLine()) != null) {
			String val[] = str.split("\\s+");
			app_key = val[0];
			
			val = bReader.readLine().split("\\s+");
			one_fours = val[1];
			time = val[2];
			
			five_tens = bReader.readLine().split("\\s+")[1];
			eleven_thirtys = bReader.readLine().split("\\s+")[1];
			thirty_sixtys = bReader.readLine().split("\\s+")[1];
			one_threem = bReader.readLine().split("\\s+")[1];
			four_tenm = bReader.readLine().split("\\s+")[1];
			eleven_thirtym = bReader.readLine().split("\\s+")[1];
			thirty_sixtym = bReader.readLine().split("\\s+")[1];
			sixtym_more = bReader.readLine().split("\\s+")[1];
			
			boolean flag = database.updateNewDeviceData(app_key, one_fours, five_tens, eleven_thirtys, 
					thirty_sixtys, one_threem, four_tenm, eleven_thirtym, thirty_sixtym, sixtym_more, time); 
			System.out.println(flag);
		}
	}
}
